package kodoblob_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestKodoBlob(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "KodoBlob Suite")
}
