package blobstore

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/digest"

	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type cloudBlobAccess struct {
	bucket            *blob.Bucket
	bucketName        string
	keyPrefix         string
	readBufferFactory ReadBufferFactory
	digestKeyFormat   digest.KeyFormat
	beforeCopy        BeforeCopyFunc
	minRefreshAge     time.Duration
	clock             clock.Clock
}

// Max number of concurrent requests that can be sent to s3 at a time.
const MAX_REQUESTS = 255

// This limits the number of concurrent requests a FindMissing() operation can run.
// This is done because sometimes FindMissing() can spawn out tends of thousands (or more) requests
// but most operations only have a few hundred. By limiting, it will allow other operations that do
// not need so many checks to begin working.

// This is done because sometimes FindMissing() will need to verify tens of thousands (or more) URLs.
// If we spawn a go thread for each one of them right away, they will eat too much CPU while waiting for
// MAX_REQUESTS limiter, and block progress of all other requests. So only spawn that many goroutines at a time.
const MAX_WORKERS_PER_FIND_MISSING = 96

var requestLimiter chan struct{}

func init() {
	requestLimiter = make(chan struct{}, MAX_REQUESTS)
	_, statsEnabled := os.LookupEnv("TRI_ENABLE_BB_STORAGE_STATS")
	if statsEnabled {
		go func() {
			for {
				if len(requestLimiter) > 0 {
					fmt.Printf("Current queue length: %d\n", len(requestLimiter))
				}
				time.Sleep(500 * time.Millisecond)
			}
		}()
	}
}

func startRequest() {
	requestLimiter <- struct{}{}
}

func endRequest() {
	<-requestLimiter
}

// NewCloudBlobAccess creates a BlobAccess that uses a cloud-based blob storage
// as a backend.
func NewCloudBlobAccess(bucket *blob.Bucket, bucketName string, keyPrefix string, readBufferFactory ReadBufferFactory, digestKeyFormat digest.KeyFormat, beforeCopy BeforeCopyFunc, minRefreshAge time.Duration, clock clock.Clock) BlobAccess {
	return &cloudBlobAccess{
		bucket:            bucket,
		bucketName:        bucketName + "/",
		keyPrefix:         keyPrefix,
		readBufferFactory: readBufferFactory,
		digestKeyFormat:   digestKeyFormat,
		beforeCopy:        beforeCopy,
		minRefreshAge:     minRefreshAge,
		clock:             clock,
	}
}

func (ba *cloudBlobAccess) Get(ctx context.Context, digest digest.Digest) buffer.Buffer {
	startRequest()

	key := ba.getKey(digest)
	result, err := ba.bucket.NewReader(ctx, key, nil)
	if err != nil {
		if gcerrors.Code(err) == gcerrors.NotFound {
			err = status.Errorf(codes.NotFound, err.Error())
		}
		endRequest()
		return buffer.NewBufferFromError(err)
	}

	b, t := buffer.WithBackgroundTask(ba.readBufferFactory.NewBufferFromReader(
		digest,
		result,
		func(dataIsValid bool) {
			if !dataIsValid {
				if err := ba.bucket.Delete(ctx, key); err == nil {
					log.Printf("Blob %#v was malformed and has been deleted from its bucket successfully", digest.String())
				} else {
					log.Printf("Blob %#v was malformed and could not be deleted from its bucket: %s", digest.String(), err)
				}
			}
		}))

	// At this stage we will attempt to update the s3 object to give it a longer lifetime.
	go func() {
		now := ba.clock.Now()
		if result.ModTime().Add(ba.minRefreshAge).Before(now) {
			// To ensure we don't deadlock here under an extremely rare case, we allow two requests to count as one here.
			// This is because touchBlob() might increase the counter by 1 causing a blocking operation. In theory all Get()
			// requests could get to this point causing a deadlock.
			// Yes we shouldn't do this, but it's too much work to engineer a better solution.
			endRequest()
			err := ba.touchBlob(ctx, key)
			startRequest()
			if err != nil {
				errorMsg := err.Error()
				if !strings.Contains(errorMsg, "PreconditionFailed") && !strings.Contains(errorMsg, "RequestCanceled") {
					fmt.Fprintln(os.Stderr, "Error refreshing object", err)
				}
			}
		}
		// We can always return true here, since we really don't care if touching the blob succeeded.
		t.Finish(nil)
		<-ctx.Done() // Wait for buffer to finish reading and close context.
		endRequest()
	}()

	return b
}

func (ba *cloudBlobAccess) Put(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
	startRequest()
	defer endRequest()
	ctx, cancel := context.WithCancel(ctx)
	w, err := ba.bucket.NewWriter(ctx, ba.getKey(digest), nil)
	if err != nil {
		cancel()
		b.Discard()
		return err
	}
	// In case of an error (e.g. network failure), we cancel before closing to
	// request the write to be aborted.
	if err = b.IntoWriter(w); err != nil {
		cancel()
		w.Close()
		return err
	}
	w.Close()
	cancel()
	return nil
}

func (ba *cloudBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	type Result struct {
		Value *digest.Digest
		Error error
	}
	now := ba.clock.Now()
	digestCount := len(digests.Items())
	var wg sync.WaitGroup
	wg.Add(digestCount)

	requestChannel := make(chan digest.Digest, MAX_WORKERS_PER_FIND_MISSING)
	resultChannel := make(chan Result, digestCount)

	for i := 0; i < MAX_WORKERS_PER_FIND_MISSING; i++ {
		go func() {
			for blobDigest := range requestChannel {
				startRequest()
				key := ba.getKey(blobDigest)
				attributes, err := ba.bucket.Attributes(ctx, key)
				endRequest()
				switch gcerrors.Code(err) {
				case gcerrors.OK:
					// If it appears we are about to expire, refresh the object in s3.
					if attributes.ModTime.Add(ba.minRefreshAge).Before(now) {
						err := ba.touchBlob(ctx, key)
						if err != nil {
							errorMsg := err.Error()
							if !strings.Contains(errorMsg, "PreconditionFailed") && !strings.Contains(errorMsg, "RequestCanceled") {
								fmt.Fprintln(os.Stderr, "Error refreshing object", err)
							}
						}
					}
					// Not missing
				case gcerrors.NotFound:
					// Missing
					resultChannel <- Result{
						Value: &blobDigest,
						Error: nil,
					}
				default:
					resultChannel <- Result{
						Value: nil,
						Error: err,
					}
				}
				wg.Done()
			}
		}()
	}

	for _, blobDigest := range digests.Items() {
		requestChannel <- blobDigest
	}

	close(requestChannel)
	wg.Wait() // Wait for all goroutines to finish.
	close(resultChannel)

	missing := digest.NewSetBuilder()
	for result := range resultChannel {
		if result.Error != nil {
			return digest.EmptySet, result.Error
		}
		missing.Add(*result.Value)
	}

	return missing.Build(), nil
}

func (ba *cloudBlobAccess) getKey(digest digest.Digest) string {
	return ba.keyPrefix + digest.GetKey(ba.digestKeyFormat)
}

func (ba *cloudBlobAccess) touchBlob(ctx context.Context, key string) error {
	startRequest()
	defer endRequest()
	// Touch the object to update its modification time, so that cloud expiration policies will be LRU.
	// TRI-NOTE: we use legacy path style, so we need to prefix the source key with the bucket name.
	return ba.bucket.Copy(ctx, key, ba.bucketName+key, &blob.CopyOptions{
		BeforeCopy: ba.beforeCopy,
	})
}
