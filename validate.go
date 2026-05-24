package fsdb

import (
	"fmt"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// validateDocname rejects Firestore-illegal document paths before they reach
// the SDK. Returns a NotFound-typed error for reserved or malformed paths;
// valid paths return nil.
//
// Why NotFound rather than InvalidArgument: read paths in this library have
// a long-established convention of "miss → ErrorIsNotFound", and an unknown
// fraction of callers handle a doc lookup by checking that helper. Returning
// NotFound for reserved/malformed names lets every read site treat the
// rejection identically to a normal miss without learning a new error class.
// The cost is that write paths (Add/AddOrReplace/Delete) also see NotFound
// for malformed names — semantically unusual, but the response is still
// "this operation didn't happen", which is what a caller passing a bad name
// needs to know. Callers that need to distinguish bad-input from no-such-doc
// can inspect the error message; the typical Firestore-flow caller doesn't.
//
// What's rejected:
//   - empty docname / empty path segment
//   - any segment matching Firestore's reserved __.*__ pattern (so __foo__,
//     __a__, __, ____, etc.)
//
// What's intentionally NOT rejected here (left to the SDK / server):
//   - oversize path / oversize segment (1500-byte cap)
//   - other Firestore-internal limits we don't replicate
func validateDocname(docname string) error {
	if docname == "" {
		return status.Error(codes.NotFound, "empty docname")
	}
	for _, seg := range strings.Split(docname, "/") {
		if seg == "" {
			return status.Error(codes.NotFound, fmt.Sprintf("empty segment in docname %q", docname))
		}
		if strings.HasPrefix(seg, "__") && strings.HasSuffix(seg, "__") {
			return status.Error(codes.NotFound, fmt.Sprintf("reserved Firestore name in segment %q", seg))
		}
	}
	return nil
}
