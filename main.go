package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"math/rand"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/storage"
	"golang.org/x/sync/errgroup"
)

func run() error {
	n := flag.Int("n", 24, "number of goroutines for uploading")
	sequential := flag.Bool("s", false, "upload sequential upload")
	verbose := flag.Bool("v", false, "show verbose")
	flag.Parse()
	if flag.NArg() != 2 {
		flag.Usage()
		return fmt.Errorf("invalid args")
	}
	src := flag.Arg(0)
	dest, err := url.ParseRequestURI(flag.Arg(1))
	if err != nil {
		return fmt.Errorf("parse dest: %w", err)
	}

	if dest.Scheme != "gs" {
		return fmt.Errorf("dest must start with gs://: %s", dest.Scheme)
	}

	var files []string

	err = fs.WalkDir(os.DirFS(src), ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		files = append(files, p)
		return nil
	})
	if err != nil {
		return fmt.Errorf("walk src: %w", err)
	}

	if len(files) == 0 {
		return nil
	}

	if !*sequential {
		rand.Shuffle(len(files), func(i, j int) {
			files[i], files[j] = files[j], files[i]
		})
	}

	ctx := context.Background()
	gcs, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("storage client: %w", err)
	}

	bucket := gcs.Bucket(dest.Hostname())

	uploadBufPool := sync.Pool{
		New: func() any {
			return make([]byte, 16*1024*1024)
		},
	}

	var count atomic.Int64
	countWidth := len(strconv.Itoa(len(files)))

	uploadsStart := time.Now()
	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(*n)
	for _, f := range files {
		f := f
		eg.Go(func() error {
			select {
			case <-ctx.Done():
				return nil
			default:
			}

			r, err := os.Open(f)
			if err != nil {
				return fmt.Errorf("open upload file: %w", err)
			}
			defer r.Close()

			name := path.Join(dest.Path, filepath.ToSlash(f))
			o := bucket.Object(name).Retryer(storage.WithPolicy(storage.RetryAlways))
			w := o.NewWriter(ctx)
			defer w.Close()

			buf := uploadBufPool.Get().([]byte)
			defer uploadBufPool.Put(buf)

			var start time.Time
			if *verbose {
				start = time.Now()
			}
			if _, err := io.CopyBuffer(w, r, buf); err != nil {
				return fmt.Errorf("upload: %w", err)
			}
			if err := w.Close(); err != nil {
				return fmt.Errorf("close writer: %w", err)
			}
			if *verbose {
				log.Printf("%*d: -> %s: %s", countWidth, count.Add(1), "gs://"+path.Join(o.BucketName(), o.ObjectName()), time.Now().Sub(start))
			}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return fmt.Errorf("uploads: %w", err)
	}
	log.Printf("total: %s", time.Now().Sub(uploadsStart))
	return nil
}

func main() {
	log.SetPrefix("gcs-upload: ")
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
