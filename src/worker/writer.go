package worker

import (
	"bufio"
	"io"
	"sync"

	"github.com/DenisCheremisov/gosnippets/golog"
)

// SyncWriter object
type SyncWriter struct {
	mutex  *sync.Mutex
	writer *bufio.Writer
}

// NewSyncWriter creates new SyncWriter
func NewSyncWriter(writer io.Writer) *SyncWriter {
	return &SyncWriter{
		mutex:  &sync.Mutex{},
		writer: bufio.NewWriter(writer),
	}
}

// Lock SyncWriter
func (sw *SyncWriter) Lock() {
	sw.mutex.Lock()
}

// Unlock SyncWriter
func (sw *SyncWriter) Unlock() {
	sw.mutex.Unlock()
}

// GetWriter from SyncWriter
func (sw *SyncWriter) GetWriter() *bufio.Writer {
	return sw.writer
}

// Finish prepares SyncWriter finish
func (sw *SyncWriter) Finish() {
	sw.mutex.Lock()
	sw.writer.Flush()
	sw.writer = nil
	sw.mutex.Unlock()
}

// Flush flushes SyncWriter
func (sw *SyncWriter) Flush() (ok bool) {
	ok = true
	sw.mutex.Lock()
	if sw.writer != nil {
		err := sw.writer.Flush()
		if err != nil {
			log.Fatal(err)
		}
	} else {
		ok = false
	}
	sw.mutex.Unlock()
	return ok
}
