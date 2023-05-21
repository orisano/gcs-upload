package main

import (
	"bufio"
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
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage of gcs-upload <dest>:\n")
		flag.PrintDefaults()
	}

	n := flag.Int("n", 24, "number of goroutines for uploading")
	verbose := flag.Bool("v", false, "show verbose output")
	bufSize := flagBytes("buf", 512*1024, "copy buffer size")
	chunkSize := flagBytes("chunk", 16*1024*1024, "upload chunk size")
	gcInterval := flag.Int("gc", 0, "gc interval")
	shuffle := flag.Bool("shuffle", false, "shuffle upload order")
	listFilePath := flag.String("l", "", "target list-file")
	dir := flag.String("d", "", "local directory containing the files to be uploaded")

	flag.Parse()
	if flag.NArg() != 1 {
		flag.Usage()
		return fmt.Errorf("invalid args")
	}

	if *listFilePath == "" && *dir == "" {
		flag.Usage()
		return fmt.Errorf("target not found: please use either -l or -d")
	}
	if *listFilePath != "" && *dir != "" {
		flag.Usage()
		return fmt.Errorf("cannot use both -l and -d")
	}

	dest, err := url.ParseRequestURI(flag.Arg(0))
	if err != nil {
		return fmt.Errorf("parse dest: %w", err)
	}

	if dest.Scheme != "gs" {
		return fmt.Errorf("dest must start with gs://: %s", dest.Scheme)
	}

	if *dir != "" {
		lf, err := writeListFile(*dir)
		if lf != "" {
			defer os.Remove(lf)
		}
		if err != nil {
			return fmt.Errorf("write list file: %w", err)
		}
		*listFilePath = lf
	}

	if *shuffle {
		lf, err := shuffleListFile(*listFilePath)
		if lf != "" {
			defer os.Remove(lf)
		}
		if err != nil {
			return fmt.Errorf("shuffle list file: %w", err)
		}
		*listFilePath = lf
	}

	listFile, err := openFile(*listFilePath)
	if err != nil {
		return fmt.Errorf("open list file: %w", err)
	}
	defer listFile.Close()

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

	uploadsStart := time.Now()
	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(*n)

	listFileScanner := bufio.NewScanner(listFile)
	for listFileScanner.Scan() {
		f := listFileScanner.Text()
		eg.Go(func() error {
			select {
			case <-ctx.Done():
				return nil
			default:
			}

			r, err := os.Open(filepath.Join(*dir, f))
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
				log.Printf("%7d: -> %s: %s", c, "gs://"+path.Join(o.BucketName(), o.ObjectName()), time.Now().Sub(start))
			}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return fmt.Errorf("uploads: %w", err)
	}
	if err := listFileScanner.Err(); err != nil {
		return fmt.Errorf("scan list file: %w", err)
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

func openFile(name string) (*os.File, error) {
	if name == "-" {
		return os.Stdin, nil
	}
	return os.Open(name)
}

func writeListFile(dir string) (string, error) {
	f, err := os.CreateTemp("", "")
	if err != nil {
		return "", fmt.Errorf("create list file: %w", err)
	}
	err = fs.WalkDir(os.DirFS(dir), ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if _, err := f.WriteString(p + "\n"); err != nil {
			return fmt.Errorf("write path: %w", err)
		}
		return nil
	})

	if err != nil {
		return f.Name(), fmt.Errorf("walk(%s): %w", dir, err)
	}
	if err := f.Close(); err != nil {
		return f.Name(), fmt.Errorf("close list file: %w", err)
	}
	return f.Name(), nil
}

func shuffleListFile(listFile string) (string, error) {
	f, err := openFile(listFile)
	if err != nil {
		return "", fmt.Errorf("open list file: %w", err)
	}
	defer f.Close()

	var files []string
	s := bufio.NewScanner(f)
	for s.Scan() {
		files = append(files, s.Text())
	}
	if err := s.Err(); err != nil {
		return "", fmt.Errorf("scan list file: %w", err)
	}
	_ = f.Close()

	rand.Shuffle(len(files), func(i, j int) {
		files[i], files[j] = files[j], files[i]
	})

	tf, err := os.CreateTemp("", "")
	if err != nil {
		return "", fmt.Errorf("create list file: %w", err)
	}
	defer tf.Close()

	for _, file := range files {
		if _, err := tf.WriteString(file + "\n"); err != nil {
			return "", fmt.Errorf("write path: %w", err)
		}
	}
	if err := tf.Close(); err != nil {
		return "", fmt.Errorf("close list file: %w", err)
	}
	return tf.Name(), nil
}
