
package blobstore

import (
  "context"
  "io"

  "google.golang.org/grpc/codes"
  "google.golang.org/grpc/status"

  "github.com/buildbarn/bb-storage/pkg/util"
)

type fallbackBlobAccess struct {
  primaryBlobAccess BlobAccess
  secondaryBlobAccess BlobAccess
}

// NewFallbackBlobAccess
func NewFallbackBlobAccess(primary BlobAccess, secondary BlobAccess) BlobAccess {
  return &fallbackBlobAccess{
    primaryBlobAccess: primary,
    secondaryBlobAccess: secondary,
  }
}

func (ba *fallbackBlobAccess) Get(ctx context.Context, digest *util.Digest) (int64, io.ReadCloser, error) {
  sz, reader, err := ba.primaryBlobAccess.Get(ctx, digest)
  if err == nil {
    return sz, reader, err
  }
  sz, reader, err = ba.secondaryBlobAccess.Get(ctx, digest)
  if err != nil {
    return sz, reader, err
  }
  // Populate our local cache then ask for it again.
  ba.primaryBlobAccess.Put(ctx, digest, sz, reader)
  return ba.primaryBlobAccess.Get(ctx, digest)
}

func (ba *fallbackBlobAccess) Put(ctx context.Context, digest *util.Digest, sizeBytes int64, r io.ReadCloser) error {
  err1 := ba.primaryBlobAccess.Put(ctx, digest, sizeBytes, r)
  getSz, getReader, getErr := ba.primaryBlobAccess.Get(ctx, digest)
  if getErr != nil {
    return getErr
  }
  if getSz != sizeBytes {
    return status.Errorf(codes.Internal, "Put blob with size %u and when retrieved got size %u", sizeBytes, getSz)
  }
  err2 := ba.secondaryBlobAccess.Put(ctx, digest, getSz, getReader)
  if err1 == nil && err2 == nil {
    return nil
  }
  if err1 == nil {
    return err1
  }
  return err2
}

func (ba *fallbackBlobAccess) Delete(ctx context.Context, digest *util.Digest) error {
  // We do not want to allow bazel clients to delete entries from CAS.
  return nil
}

type fallbackFindMissingResults struct {
  missing map[string]bool
  err     error
}

func (ba *fallbackBlobAccess) FindMissing(ctx context.Context, digests []*util.Digest) ([]*util.Digest, error) {
  // Forward FindMissing() to both implementations.
  secondaryResultsChan := make(chan fallbackFindMissingResults, 1)
  go func() {
    missing, err := ba.secondaryBlobAccess.FindMissing(ctx, digests)
    missingDigestSet := map[string]bool{}
    if err != nil {
      secondaryResultsChan <- fallbackFindMissingResults{missing: missingDigestSet, err: err}
      return
    }
    for _, digest := range missing {
      missingDigestSet[digest.GetHashString()] = true
    }
    secondaryResultsChan <- fallbackFindMissingResults{missing: missingDigestSet, err: err}
  }()
  primaryMissing, err := ba.primaryBlobAccess.FindMissing(ctx, digests)

  if err != nil || len(primaryMissing) == 0 {
    // Happy path where the primary blob access has all the items (or error), so ignore the channel and return.
    return primaryMissing, err
  }

  secondaryResults := <-secondaryResultsChan
  if secondaryResults.err != nil {
    return primaryMissing, err
  }

  actualMissing := []*util.Digest{}
  for _, digest := range primaryMissing {
    if _, ok := secondaryResults.missing[digest.GetHashString()]; ok {
      // If our primaryBlobAccess is missing it, but our secondary has it, skip it.
      continue
    }
    // Both entries are missing.
    actualMissing = append(actualMissing, digest)
  }

  return actualMissing, nil
}