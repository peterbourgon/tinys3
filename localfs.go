package main

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

var (
	ErrNoSuchBucket   = errors.New("no such bucket")
	ErrBucketNotEmpty = errors.New("bucket not empty")
)

type LocalFS struct {
	root string
}

func NewLocalFS(root string) *LocalFS {
	return &LocalFS{
		root: root,
	}
}

//
//
//

type BucketInfo struct {
	Name         string
	CreationDate time.Time
}

func (f *LocalFS) bucketPath(name string) string {
	return filepath.Join(f.root, name)
}

func (f *LocalFS) ListBuckets() ([]BucketInfo, error) {
	dents, err := os.ReadDir(f.root)
	if err != nil {
		return nil, err
	}
	var out []BucketInfo
	for _, de := range dents {
		if !de.IsDir() {
			continue
		}
		st, err := os.Stat(filepath.Join(f.root, de.Name()))
		if err != nil {
			continue
		}
		out = append(out, BucketInfo{Name: de.Name(), CreationDate: st.ModTime()})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func (f *LocalFS) MakeBucket(name string) error {
	p := f.bucketPath(name)
	return os.MkdirAll(p, 0o755)
}

func (f *LocalFS) DeleteBucket(name string) error {
	p := f.bucketPath(name)
	// ensure empty
	empty := true
	_ = filepath.WalkDir(p, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d == nil {
			return nil
		}
		if d.IsDir() {
			return nil
		}
		empty = false
		return fs.SkipAll
	})
	if !empty {
		return ErrBucketNotEmpty
	}
	return os.Remove(p)
}

//
//
//

type ListV2 struct {
	Contents              []ObjInfo
	CommonPrefixes        []string
	IsTruncated           bool
	NextContinuationToken string
}

type ObjInfo struct {
	Key          string
	Size         int64
	ETag         string
	LastModified time.Time
}

func (f *LocalFS) objPath(bucket, key string) string {
	return filepath.Join(f.root, bucket, filepath.FromSlash(key))
}

func (f *LocalFS) ensureBucket(bucket string) error {
	p := f.bucketPath(bucket)
	_, err := os.Stat(p)
	if err == nil {
		return nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return ErrNoSuchBucket
	}
	return err
}

func (f *LocalFS) PutObject(bucket, key string, body io.Reader, n int64) (string, int64, time.Time, error) {
	if err := f.MakeBucket(bucket); err != nil {
		return "", 0, time.Time{}, err
	}
	p := f.objPath(bucket, key)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return "", 0, time.Time{}, err
	}

	// write to temp + rename
	tmp := p + ".tmp"
	wf, err := os.Create(tmp)
	if err != nil {
		return "", 0, time.Time{}, err
	}
	h := md5.New()
	nw, err := io.Copy(io.MultiWriter(wf, h), body)
	cerr := wf.Close()
	if err == nil {
		err = cerr
	}
	if err != nil {
		_ = os.Remove(tmp)
		return "", 0, time.Time{}, err
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		_ = os.Remove(tmp)
		return "", 0, time.Time{}, err
	}
	if err := os.Rename(tmp, p); err != nil {
		_ = os.Remove(tmp)
		return "", 0, time.Time{}, err
	}
	etag := hex.EncodeToString(h.Sum(nil))
	st, _ := os.Stat(p)
	return etag, nw, st.ModTime(), nil
}

func (f *LocalFS) GetObject(bucket, key string, br *ByteRange) (io.ReadCloser, int64, string, time.Time, error) {
	if err := f.ensureBucket(bucket); err != nil {
		return nil, 0, "", time.Time{}, err
	}
	p := f.objPath(bucket, key)
	fobj, err := os.Open(p)
	if err != nil {
		return nil, 0, "", time.Time{}, err
	}
	st, err := fobj.Stat()
	if err != nil {
		fobj.Close()
		return nil, 0, "", time.Time{}, err
	}
	etag, _, _ := md5HexOfFile(p)
	if br == nil {
		return fobj, st.Size(), etag, st.ModTime(), nil
	}
	// Section reader for range
	var start, length int64
	if br.Start == -1 { // suffix
		if br.Length > st.Size() {
			br.Length = st.Size()
		}
		start = st.Size() - br.Length
		length = br.Length
	} else if br.End == -1 { // open-ended
		start = br.Start
		length = st.Size() - start
	} else {
		start = br.Start
		length = br.Length
	}
	if start < 0 || start >= st.Size() {
		fobj.Close()
		return nil, 0, "", time.Time{}, os.ErrNotExist
	}
	// Wrap a SectionReader into a ReadCloser
	sr := io.NewSectionReader(fobj, start, length)
	return struct {
		io.Reader
		io.Closer
	}{Reader: sr, Closer: fobj}, length, etag, st.ModTime(), nil
}

func (f *LocalFS) HeadObject(bucket, key string) (int64, string, time.Time, error) {
	if err := f.ensureBucket(bucket); err != nil {
		return 0, "", time.Time{}, err
	}
	p := f.objPath(bucket, key)
	st, err := os.Stat(p)
	if err != nil {
		return 0, "", time.Time{}, err
	}
	etag, _, _ := md5HexOfFile(p)
	return st.Size(), etag, st.ModTime(), nil
}

func (f *LocalFS) DeleteObject(bucket, key string) error {
	if err := f.ensureBucket(bucket); err != nil {
		return err
	}
	p := f.objPath(bucket, key)
	return os.Remove(p)
}

// Basic, in-memory paginated V2 listing from filesystem
func (f *LocalFS) ListObjectsV2(bucket, prefix, delimiter, startAfter, continuationToken string, maxKeys int) (ListV2, error) {
	if err := f.ensureBucket(bucket); err != nil {
		return ListV2{}, err
	}
	root := f.objPath(bucket, "")
	var all []ObjInfo
	walkErr := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, _ := filepath.Rel(root, p)
		key := filepath.ToSlash(rel)
		if prefix != "" && !strings.HasPrefix(key, prefix) {
			return nil
		}
		st, err := os.Stat(p)
		if err != nil {
			return nil
		}
		etag, _, _ := md5HexOfFile(p)
		all = append(all, ObjInfo{Key: key, Size: st.Size(), ETag: etag, LastModified: st.ModTime()})
		return nil
	})
	if walkErr != nil && !errors.Is(walkErr, os.ErrNotExist) {
		return ListV2{}, walkErr
	}

	sort.Slice(all, func(i, j int) bool { return all[i].Key < all[j].Key })
	start := 0
	if continuationToken != "" {
		// token is last key seen; start after it
		for i := range all {
			if all[i].Key > continuationToken {
				start = i
				break
			}
		}
	}
	if startAfter != "" {
		for i := start; i < len(all); i++ {
			if all[i].Key > startAfter {
				start = i
				break
			}
		}
	}
	end := min(start+maxKeys, len(all))
	page := all[start:end]

	res := ListV2{}
	if delimiter == "" {
		res.Contents = append(res.Contents, page...)
	} else {
		// Split into objects and common prefixes
		seen := map[string]struct{}{}
		for _, oi := range page {
			rest := strings.TrimPrefix(oi.Key, prefix)
			if i := strings.Index(rest, delimiter); i >= 0 {
				pref := prefix + rest[:i+1]
				if _, ok := seen[pref]; !ok {
					res.CommonPrefixes = append(res.CommonPrefixes, pref)
					seen[pref] = struct{}{}
				}
				continue
			}
			res.Contents = append(res.Contents, oi)
		}
	}
	if end < len(all) {
		res.IsTruncated = true
		res.NextContinuationToken = all[end-1].Key
	}
	return res, nil
}

// byteRange is duplicated here for build independence from package main
// Kept minimal: only fields used by FS.GetObject

func md5HexOfFile(p string) (string, int64, error) {
	f, err := os.Open(p)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	h := md5.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}
