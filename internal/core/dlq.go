package core

import "strings"

const DLQSuffix = ".dlq"

// IsDLQQueue reports whether queue is a dead-letter queue (name ends with .dlq).
func IsDLQQueue(queue string) bool {
	return strings.HasSuffix(queue, DLQSuffix)
}
