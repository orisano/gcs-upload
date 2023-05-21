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
	"runtime"
	"strconv"
	"strings"
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
	bufSize := flagBytes("buf", 512*1024, "buffer size")
	chunkSize := flagBytes("chunk", 16*1024*1024, "chunk size")
	gcInterval := flag.Int("gc", 0, "gc interval")

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
			return make([]byte, *bufSize)
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

			r, err := os.Open(filepath.Join(src, f))
			if err != nil {
				return fmt.Errorf("open upload file: %w", err)
			}
			defer r.Close()

			name := path.Join(dest.Path[1:], filepath.ToSlash(f))
			o := bucket.Object(name).Retryer(storage.WithPolicy(storage.RetryAlways))
			w := o.NewWriter(ctx)
			w.ChunkSize = int(*chunkSize)
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
			c := count.Add(1)
			if *gcInterval > 0 && int(c)%*gcInterval == 0 {
				runtime.GC()
			}
			if *verbose {
				log.Printf("%*d: -> %s: %s", countWidth, c, "gs://"+path.Join(o.BucketName(), o.ObjectName()), time.Now().Sub(start))
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

func flagBytes(name string, value uint64, usage string) *uint64 {
	p := new(uint64)
	*p = value
	flag.Var((*bytesValue)(p), name, usage)
	return p
}

var bytesUnits = []struct {
	suffix string
	value  uint64
}{
	{"m", 1 * 1024 * 1024},
	{"k", 1 * 1024},
	{"mb", 1 * 1024 * 1024},
	{"kb", 1 * 1024},
	{"b", 1},
	{"", 1},
}

type bytesValue uint64

func (b *bytesValue) String() string {
	for _, u := range bytesUnits {
		if uint64(*b) >= u.value {
			return strconv.FormatUint(uint64(*b)/u.value, 10) + u.suffix
		}
	}
	return "0"
}

func (b *bytesValue) Set(s string) error {
	x := strings.ToLower(s)
	for _, u := range bytesUnits {
		if !strings.HasSuffix(x, u.suffix) {
			continue
		}
		v, err := strconv.ParseUint(strings.TrimSuffix(x, u.suffix), 10, 64)
		if err != nil {
			return fmt.Errorf("parse(%s): %w", s, err)
		}
		*b = bytesValue(v * u.value)
		return nil
	}
	panic("unreachable")
}
