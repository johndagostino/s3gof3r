package main

import (
	"github.com/rlmcpherson/s3gof3r"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

type syncOpts struct {
	Path   string `short:"p" long:"path" description:"Path to directory." no-ini:"true"`
	Bucket string `long:"bucket" short:"b" description:"S3 bucket" required:"true" no-ini:"true"`
	Prefix string `long:"prefix" short:"k" description:"Key Prefix" required:"false" no-ini:"true"`
	DataOpts
	CommonOpts
	UpOpts
}

var sync syncOpts

func (sync *syncOpts) Execute(args []string) (err error) {
	conf := new(s3gof3r.Config)
	*conf = *s3gof3r.DefaultConfig

	k, err := getAWSKeys()
	if err != nil {
		return
	}

	s3 := s3gof3r.New(sync.EndPoint, k)
	b := s3.Bucket(sync.Bucket)
	conf.Concurrency = sync.Concurrency

	if sync.NoSSL {
		conf.Scheme = "http"
	}

	conf.PartSize = sync.PartSize
	conf.Md5Check = !sync.NoMd5
	s3gof3r.SetLogger(os.Stderr, "", log.LstdFlags, sync.Debug)

	if sync.Header == nil {
		sync.Header = make(http.Header)
	}

	walkpath := func(path string, f os.FileInfo, err error) error {
		r, err := os.Open(path)
		rel, err := filepath.Rel(sync.Path, path)

		s := []string{sync.Prefix, rel}
		key := strings.Join(s, "")

		defer r.Close()

		w, err := b.PutWriter(key, ACL(put.Header, put.ACL), conf)

		if err != nil {
			return nil
		}
		if _, err = io.Copy(w, r); err != nil {
			return nil
		}
		if err = w.Close(); err != nil {
			return nil
		}
		return nil
	}

	filepath.Walk(sync.Path, walkpath)
	return
}

func init() {
	_, err := parser.AddCommand("sync", "sync a directory to S3", "sync (upload) data to S3", &sync)
	if err != nil {
		log.Fatal(err)
	}
}
