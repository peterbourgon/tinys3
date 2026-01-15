package main

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

type ByteRange struct {
	Start  int64
	End    int64
	Length int64
}

func ParseRange(h string) (*ByteRange, int) {
	if h == "" {
		return nil, http.StatusOK
	}
	// Expect: bytes=start-end
	if !strings.HasPrefix(h, "bytes=") {
		return nil, http.StatusRequestedRangeNotSatisfiable
	}
	val := strings.TrimPrefix(h, "bytes=")
	parts := strings.Split(val, ",")
	if len(parts) != 1 {
		return nil, http.StatusRequestedRangeNotSatisfiable
	}
	se := strings.SplitN(strings.TrimSpace(parts[0]), "-", 2)
	if len(se) != 2 {
		return nil, http.StatusRequestedRangeNotSatisfiable
	}
	var br ByteRange
	if se[0] == "" {
		// suffix length
		l, err := strconv.ParseInt(se[1], 10, 64)
		if err != nil || l <= 0 {
			return nil, http.StatusRequestedRangeNotSatisfiable
		}
		br.Start = -1 // suffix marker
		br.End = -1
		br.Length = l
		return &br, http.StatusPartialContent
	}
	start, err := strconv.ParseInt(se[0], 10, 64)
	if err != nil || start < 0 {
		return nil, http.StatusRequestedRangeNotSatisfiable
	}
	if se[1] == "" {
		br.Start = start
		br.End = -1
		br.Length = -1
		return &br, http.StatusPartialContent
	}
	end, err := strconv.ParseInt(se[1], 10, 64)
	if err != nil || end < start {
		return nil, http.StatusRequestedRangeNotSatisfiable
	}
	br.Start = start
	br.End = end
	br.Length = end - start + 1
	return &br, http.StatusPartialContent
}

func (br *ByteRange) ContentRange(objSize int64) string {
	if br == nil {
		return ""
	}
	var start, end int64
	if br.Start == -1 { // suffix
		if br.Length > objSize {
			br.Length = objSize
		}
		start = objSize - br.Length
		end = objSize - 1
	} else if br.End == -1 { // open-ended
		start = br.Start
		end = objSize - 1
		br.Length = end - start + 1
	} else {
		start = br.Start
		end = br.End
	}
	return fmt.Sprintf("bytes %d-%d/%d", start, end, objSize)
}
