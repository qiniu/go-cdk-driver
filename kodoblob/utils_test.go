package kodoblob_test

import (
	"bytes"
	"io"
	"math/rand"
	"time"

	. "github.com/onsi/gomega"
)

func randData(n int) []byte {
	var buf bytes.Buffer
	_, err := io.Copy(&buf, io.LimitReader(rand.New(rand.NewSource(time.Now().UnixNano())), int64(n)))
	Expect(err).NotTo(HaveOccurred())

	return buf.Bytes()
}
