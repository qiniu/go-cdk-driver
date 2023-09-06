package kodoblob_test

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type HandlerFunc func(http.ResponseWriter, *http.Request, uint32)

type mockServer struct {
	server  *httptest.Server
	handler HandlerFunc
	mux     *http.ServeMux
	max     uint32
	n       uint32
}

func newMockServer() *mockServer {
	return newMockServerWithHandler(nil, 0)
}

func newMockServerWithHandler(handler HandlerFunc, max uint32) *mockServer {
	ms := new(mockServer)
	ms.SetHandler(handler, max)
	ms.server = httptest.NewServer(ms.createMockHandler())
	return ms
}

func newMockServerWithMux(f func(*http.ServeMux), max uint32) *mockServer {
	ms := new(mockServer)
	ms.WithMux(f, max)
	ms.server = httptest.NewServer(ms.createMockHandler())
	return ms
}

func (ms *mockServer) createMockHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer GinkgoRecover()
		n := atomic.AddUint32(&ms.n, 1) - 1
		if ms.handler != nil && n < ms.max {
			ms.handler(w, r, n)
		} else if ms.mux != nil && n < ms.max {
			ms.mux.ServeHTTP(w, r)
		} else {
			Fail("should not reach here")
		}
	})
}

func (ms *mockServer) Host() string {
	u, err := url.Parse(ms.URL())
	Expect(err).NotTo(HaveOccurred())
	return u.Host
}

func (ms *mockServer) URL() string {
	return ms.server.URL
}

func (ms *mockServer) Close() {
	ms.server.Close()
}

func (ms *mockServer) SetHandler(handler HandlerFunc, max uint32) {
	ms.handler = handler
	ms.max = max
}

func (ms *mockServer) WithMux(f func(*http.ServeMux), max uint32) {
	ms.mux = http.NewServeMux()
	ms.max = max
	f(ms.mux)
}
