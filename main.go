package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/peterbourgon/ff/v4"
	"github.com/peterbourgon/ff/v4/ffhelp"
	"github.com/peterbourgon/unixtransport"
)

func main() {
	fs := ff.NewFlagSet("tinys3")
	var (
		flagAddr = fs.String('a', "addr", ":1234", "S3-compatible endpoint listen address")
		flagRoot = fs.String('r', "root", "/tmp/tinys3", "root directory for storage")
	)

	err := ff.Parse(fs, os.Args[1:])
	switch {
	case errors.Is(err, ff.ErrHelp):
		fmt.Fprintf(os.Stdout, "%s\n", ffhelp.Flags(fs, "tinys3 [FLAGS]"))
		os.Exit(0)
	case err != nil:
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	log.SetFlags(log.LUTC | log.Ltime | log.Lmicroseconds)

	if err := os.MkdirAll(*flagRoot, 0o755); err != nil {
		log.Fatalf("mkdir root: %v", err)
	}

	objectStore := NewLocalFS(*flagRoot)
	mux := NewRouter(objectStore)

	ctx := context.Background()

	ln, err := unixtransport.ListenURI(ctx, *flagAddr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	server := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	log.Printf("tinys3 serving %s on %s", *flagRoot, *flagAddr)
	log.Printf("usage: aws --endpoint-url=URL s3 ls s3://")

	log.Fatal(server.Serve(ln))
}
