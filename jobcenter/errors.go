package jobcenter

import "fmt"

type classifiedError struct {
	err       error
	code      string
	retryable bool
}

func (e *classifiedError) Error() string {
	if e.err == nil {
		return e.code
	}
	return fmt.Sprintf("%s: %v", e.code, e.err)
}
func (e *classifiedError) Unwrap() error { return e.err }

func Retryable(err error, code string) error {
	return &classifiedError{err: err, code: code, retryable: true}
}
func NonRetryable(err error, code string) error {
	return &classifiedError{err: err, code: code, retryable: false}
}

func IsRetryable(err error) (code string, ok bool) {
	if ce, ok2 := err.(*classifiedError); ok2 {
		return ce.code, ce.retryable
	}
	return "", false
}
