package fsdb

import (
	"context"
	"errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func ErrorIsNotFound(err error) bool {
	return status.Code(err) == codes.NotFound
}

func ErrorIsAlreadyExists(err error) bool {
	return status.Code(err) == codes.AlreadyExists
}

func ErrorIsCanceled(err error) bool {
	if errors.Is(err, context.Canceled) {
		return true
	}

	for ; err != nil; err = errors.Unwrap(err) {
		if status.Code(err) == codes.Canceled {
			return true
		}
	}

	return false
}
