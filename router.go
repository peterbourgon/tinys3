package main

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log"
	"mime"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type ObjectStore interface {
	ListBuckets() ([]BucketInfo, error)
	MakeBucket(name string) error
	DeleteBucket(name string) error
	ListObjectsV2(bucket, prefix, delimiter, startAfter, continuationToken string, maxKeys int) (ListV2, error)
	PutObject(bucket, key string, body io.Reader, n int64) (etag string, size int64, mod time.Time, err error)
	GetObject(bucket, key string, rng *ByteRange) (rc io.ReadCloser, size int64, etag string, mod time.Time, err error)
	HeadObject(bucket, key string) (size int64, etag string, mod time.Time, err error)
	DeleteObject(bucket, key string) error
}

//
//
//

func NewRouter(objectStore ObjectStore) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// Root: ListBuckets
		if r.URL.Path == "/" {
			if r.Method != http.MethodGet {
				writeS3Error(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "")
				return
			}
			buckets, err := objectStore.ListBuckets()
			if err != nil {
				writeS3Error(w, http.StatusInternalServerError, "InternalError", err.Error())
				return
			}
			resp := ListAllMyBucketsResult{
				XMLName: xml.Name{Local: "ListAllMyBucketsResult"},
				Xmlns:   s3ns,
				Owner:   Owner{ID: "miniminio", DisplayName: "miniminio"},
			}
			for _, b := range buckets {
				resp.Buckets.Bucket = append(resp.Buckets.Bucket, Bucket{Name: b.Name, CreationDate: b.CreationDate.UTC().Format(time.RFC3339)})
			}
			writeXML(w, http.StatusOK, resp)
			return
		}

		// Path-style bucket or object
		parts := strings.Split(strings.TrimPrefix(path.Clean(r.URL.Path), "/"), "/")
		if len(parts) == 0 || parts[0] == "" {
			writeS3Error(w, http.StatusNotFound, "NoSuchBucket", "")
			return
		}
		bucket := parts[0]
		key := ""
		if len(parts) > 1 {
			key = strings.Join(parts[1:], "/")
		}

		if key == "" {
			// Bucket-level ops
			switch r.Method {
			case http.MethodPut:
				if err := objectStore.MakeBucket(bucket); err != nil {
					writeFsErr(w, err)
					return
				}
				log.Printf("BUCKET PUT %s", bucket)
				w.WriteHeader(http.StatusOK)
				return
			case http.MethodDelete:
				if err := objectStore.DeleteBucket(bucket); err != nil {
					writeFsErr(w, err)
					return
				}
				log.Printf("BUCKET DELETE %s", bucket)
				w.WriteHeader(http.StatusNoContent)
				return
			case http.MethodGet:
				// Listing V2 only (basic)
				if r.URL.Query().Get("list-type") != "2" && r.URL.RawQuery != "" {
					// If any list params exist but not v2, signal NotImplemented
					writeS3Error(w, http.StatusNotImplemented, "NotImplemented", "only list-type=2 supported")
					return
				}
				prefix := r.URL.Query().Get("prefix")
				delimiter := r.URL.Query().Get("delimiter")
				startAfter := r.URL.Query().Get("start-after")
				cont := r.URL.Query().Get("continuation-token")
				maxKeys := atoiDefault(r.URL.Query().Get("max-keys"), 1000)
				res, err := objectStore.ListObjectsV2(bucket, prefix, delimiter, startAfter, cont, maxKeys)
				if err != nil {
					writeFsErr(w, err)
					return
				}
				resp := ListBucketResultV2{
					Xmlns:                 s3ns,
					Name:                  bucket,
					Prefix:                prefix,
					Delimiter:             delimiter,
					MaxKeys:               maxKeys,
					KeyCount:              len(res.Contents),
					IsTruncated:           res.IsTruncated,
					ContinuationToken:     cont,
					NextContinuationToken: res.NextContinuationToken,
				}
				for _, c := range res.Contents {
					resp.Contents = append(resp.Contents, Content{
						Key:          c.Key,
						LastModified: c.LastModified.UTC().Format(time.RFC3339),
						ETag:         fmt.Sprintf("\"%s\"", c.ETag),
						Size:         c.Size,
					})
				}
				for _, p := range res.CommonPrefixes {
					resp.CommonPrefixes = append(resp.CommonPrefixes, CommonPrefix{Prefix: p})
				}
				writeXML(w, http.StatusOK, resp)
				log.Printf("LIST %s prefix=%q delimiter=%q startAfter=%q continuationToken=%q maxKeys=%d", bucket, prefix, delimiter, startAfter, cont, maxKeys)
				return
			default:
				writeS3Error(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "")
				return
			}
		}

		// Object-level ops
		switch r.Method {
		case http.MethodPut:
			// Auto-create bucket if missing
			_ = objectStore.MakeBucket(bucket)

			cl := r.ContentLength
			etag, n, mod, err := objectStore.PutObject(bucket, key, r.Body, cl)
			if err != nil {
				writeFsErr(w, err)
				return
			}
			log.Printf("PUT %s/%s size=%d etag=%s", bucket, key, n, etag)
			w.Header().Set("ETag", fmt.Sprintf("\"%s\"", etag))
			w.Header().Set("Last-Modified", mod.UTC().Format(http.TimeFormat))
			w.WriteHeader(http.StatusOK)
			return

		case http.MethodGet:
			br, _ := ParseRange(r.Header.Get("Range"))
			rc, size, etag, mod, err := objectStore.GetObject(bucket, key, br)
			if err != nil {
				writeFsErr(w, err)
				return
			}
			defer rc.Close()

			ctype := mime.TypeByExtension(filepath.Ext(key))
			if ctype == "" {
				ctype = "application/octet-stream"
			}

			w.Header().Set("ETag", fmt.Sprintf("\"%s\"", etag))
			w.Header().Set("Last-Modified", mod.UTC().Format(http.TimeFormat))
			w.Header().Set("Accept-Ranges", "bytes")
			w.Header().Set("Content-Type", ctype)

			var written int64
			if br != nil {
				w.Header().Set("Content-Range", br.ContentRange(size))
				w.WriteHeader(http.StatusPartialContent)
				written, err = io.CopyN(w, rc, br.Length)
			} else {
				w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
				written, err = io.Copy(w, rc)
			}
			if err != nil && !errors.Is(err, io.EOF) {
				log.Printf("GET copy error: %v", err)
			}
			_ = written
			log.Printf("GET %s/%s size=%d etag=%s written=%d", bucket, key, size, etag, written)
			return

		case http.MethodHead:
			size, etag, mod, err := objectStore.HeadObject(bucket, key)
			if err != nil {
				writeFsErr(w, err)
				return
			}
			w.Header().Set("ETag", fmt.Sprintf("\"%s\"", etag))
			w.Header().Set("Last-Modified", mod.UTC().Format(http.TimeFormat))
			w.Header().Set("Accept-Ranges", "bytes")
			w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
			w.WriteHeader(http.StatusOK)
			return

		case http.MethodDelete:
			if err := objectStore.DeleteObject(bucket, key); err != nil {
				writeFsErr(w, err)
				return
			}
			log.Printf("DELETE %s/%s", bucket, key)
			w.WriteHeader(http.StatusNoContent)
			return
		default:
			writeS3Error(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "")
			return
		}
	})

	return mux
}

//
//
//

const s3ns = "http://s3.amazonaws.com/doc/2006-03-01/"

func atoiDefault(s string, def int) int {
	if s == "" {
		return def
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < 0 {
		return def
	}
	return n
}

func writeFsErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, os.ErrNotExist):
		writeS3Error(w, http.StatusNotFound, "NoSuchKey", err.Error())
	case errors.Is(err, ErrNoSuchBucket):
		writeS3Error(w, http.StatusNotFound, "NoSuchBucket", err.Error())
	case errors.Is(err, ErrBucketNotEmpty):
		writeS3Error(w, http.StatusConflict, "BucketNotEmpty", err.Error())
	default:
		writeS3Error(w, http.StatusInternalServerError, "InternalError", err.Error())
	}
}

func writeS3Error(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	xml.NewEncoder(w).Encode(ErrorResponse{Code: code, Message: msg, Xmlns: s3ns})
}

func writeXML(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	enc := xml.NewEncoder(w)
	enc.Indent("", "  ")
	enc.Encode(v)
}
