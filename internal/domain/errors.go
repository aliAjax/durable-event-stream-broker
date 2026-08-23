package domain

import "fmt"

type ErrorCode string

const (
	ErrInvalid     ErrorCode = "INVALID_ARGUMENT"
	ErrNotFound    ErrorCode = "NOT_FOUND"
	ErrConflict    ErrorCode = "CONFLICT"
	ErrFenced      ErrorCode = "FENCED"
	ErrQuota       ErrorCode = "QUOTA_EXCEEDED"
	ErrUnavailable ErrorCode = "UNAVAILABLE"
)

type BrokerError struct {
	Code    ErrorCode
	Message string
}

func (e *BrokerError) Error() string     { return fmt.Sprintf("%s: %s", e.Code, e.Message) }
func E(code ErrorCode, msg string) error { return &BrokerError{Code: code, Message: msg} }
