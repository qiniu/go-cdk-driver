package kodoblob_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	_ "github.com/bachue/go-cloud-dev-qiniu-driver/kodoblob"
	"gocloud.dev/blob"
)

var _ = Describe("KodoBlob", func() {
	const (
		accessKey  = "testaccesskey"
		secretKey  = "testsecretkey"
		bucketName = "fakebucketname"
	)
	var (
		bucket      *blob.Bucket
		ucServer    *mockServer
		ioServer    *mockServer
		ioSrcServer *mockServer
		rsServer    *mockServer
		rsfServer   *mockServer
		upServer    *mockServer
		apiServer   *mockServer
	)
	newBucket := func() *blob.Bucket {
		values := make(url.Values)
		values.Set("bucketHost", ucServer.URL())
		bucket, err := blob.OpenBucket(context.Background(), "kodo://"+accessKey+":"+secretKey+"@"+bucketName+"?"+values.Encode())
		Expect(err).NotTo(HaveOccurred())
		return bucket
	}
	BeforeEach(func() {
		os.RemoveAll(filepath.Join(os.TempDir(), "qiniu-golang-sdk"))
		ioServer = newMockServer()
		ioSrcServer = newMockServer()
		rsServer = newMockServer()
		rsfServer = newMockServer()
		upServer = newMockServer()
		apiServer = newMockServer()
		ucServer = newMockServerWithMux(func(mux *http.ServeMux) {
			mux.HandleFunc("/v2/query", func(w http.ResponseWriter, r *http.Request) {
				Expect(r.Method).To(Equal(http.MethodGet))
				Expect(r.URL.Path).To(Equal("/v2/query"))
				Expect(r.URL.Query().Get("ak")).To(Equal(accessKey))
				Expect(r.URL.Query().Get("bucket")).To(Equal(bucketName))

				err := json.NewEncoder(w).Encode(map[string]any{
					"region": "z0", "ttl": 3600,
					"io":     map[string]map[string][]string{"src": {"main": {ioServer.Host()}}},
					"io_src": map[string]map[string][]string{"src": {"main": {ioSrcServer.Host()}}},
					"up":     map[string]map[string][]string{"src": {"main": {upServer.Host()}}},
					"rs":     map[string]map[string][]string{"src": {"main": {rsServer.Host()}}},
					"rsf":    map[string]map[string][]string{"src": {"main": {rsfServer.Host()}}},
					"uc":     map[string]map[string][]string{"src": {"main": {ucServer.Host()}}},
					"api":    map[string]map[string][]string{"src": {"main": {apiServer.Host()}}},
				})
				Expect(err).NotTo(HaveOccurred())
			})
			mux.HandleFunc("/v4/query", func(w http.ResponseWriter, r *http.Request) {
				Expect(r.Method).To(Equal(http.MethodGet))
				Expect(r.URL.Path).To(Equal("/v4/query"))
				Expect(r.URL.Query().Get("ak")).To(Equal(accessKey))
				Expect(r.URL.Query().Get("bucket")).To(Equal(bucketName))

				err := json.NewEncoder(w).Encode(map[string][]map[string]any{
					"hosts": {
						{
							"region": "z0", "ttl": 3600,
							"io":     map[string][]string{"domains": {ioServer.Host()}},
							"io_src": map[string][]string{"domains": {ioSrcServer.Host()}},
							"up":     map[string][]string{"domains": {upServer.Host()}},
							"rs":     map[string][]string{"domains": {rsServer.Host()}},
							"rsf":    map[string][]string{"domains": {rsfServer.Host()}},
							"uc":     map[string][]string{"domains": {ucServer.Host()}},
							"api":    map[string][]string{"domains": {apiServer.Host()}},
						},
					},
				})
				Expect(err).NotTo(HaveOccurred())
			})
		}, 1)
		bucket = newBucket()
	})
	AfterEach(func() {
		bucket.Close()
		ucServer.Close()
		apiServer.Close()
		rsServer.Close()
		rsfServer.Close()
		upServer.Close()
		ioServer.Close()
		ioSrcServer.Close()
	})

	Context("ListFiles", func() {
		It("should list all files", func(ctx context.Context) {
			rsfServer.SetHandler(func(w http.ResponseWriter, r *http.Request, n uint32) {
				Expect(r.Method).To(Equal(http.MethodPost))
				Expect(r.URL.Path).To(Equal("/list"))
				Expect(r.URL.Query().Get("bucket")).To(Equal(bucketName))
				Expect(r.URL.Query().Get("limit")).To(Equal("1000"))

				if n > 5 {
					Fail("should not list more than 5 times")
				} else if n > 0 {
					Expect(r.URL.Query().Get("marker")).To(Equal(fmt.Sprintf("marker_%d", n-1)))
				} else {
					Expect(r.URL.Query().Has("marker")).To(BeFalse())
				}

				items := make([]map[string]any, 0, 1000)
				for i := uint32(0); i < 1000; i++ {
					items = append(items, map[string]any{
						"key":      fmt.Sprintf("data_%05d", n*1000+i),
						"hash":     fmt.Sprintf("hash_%05d", n*1000+i),
						"md5":      fmt.Sprintf("md5_%05d", n*1000+i),
						"fsize":    n*1000 + i,
						"mimeType": "text/plain",
						"putTime":  time.Now().UnixNano() / 100,
					})
				}
				responseBodyJson := map[string]any{"items": items}
				if n < 5 {
					responseBodyJson["marker"] = fmt.Sprintf("marker_%d", n)
				}
				err := json.NewEncoder(w).Encode(responseBodyJson)
				Expect(err).NotTo(HaveOccurred())
			}, 6)
			listIter := bucket.List(nil)
			objectCount := 0
			for {
				object, err := listIter.Next(ctx)
				if err != nil {
					if err == io.EOF {
						break
					}
					Expect(err).NotTo(HaveOccurred())
				}
				Expect(object.Key).To(Equal(fmt.Sprintf("data_%05d", objectCount)))
				Expect(object.Size).To(Equal(int64(objectCount)))
				Expect(object.MD5).To(Equal([]byte(fmt.Sprintf("md5_%05d", objectCount))))
				Expect(object.IsDir).To(BeFalse())
				objectCount += 1
			}
			Expect(objectCount).To(Equal(6000))
		})

		It("should list all files with prefix", func(ctx context.Context) {
			rsfServer.SetHandler(func(w http.ResponseWriter, r *http.Request, n uint32) {
				Expect(r.Method).To(Equal(http.MethodPost))
				Expect(r.URL.Path).To(Equal("/list"))
				Expect(r.URL.Query().Get("bucket")).To(Equal(bucketName))
				Expect(r.URL.Query().Get("limit")).To(Equal("1000"))
				Expect(r.URL.Query().Get("prefix")).To(Equal("data/"))

				items := make([]map[string]any, 0, 1000)
				for i := uint32(0); i < 1000; i++ {
					items = append(items, map[string]any{
						"key":      fmt.Sprintf("data_%05d", n*1000+i),
						"hash":     fmt.Sprintf("hash_%05d", n*1000+i),
						"md5":      fmt.Sprintf("md5_%05d", n*1000+i),
						"fsize":    n*1000 + i,
						"mimeType": "text/plain",
						"putTime":  time.Now().UnixNano() / 100,
					})
				}
				responseBodyJson := map[string][]map[string]any{"items": items}
				err := json.NewEncoder(w).Encode(responseBodyJson)
				Expect(err).NotTo(HaveOccurred())
			}, 1)
			listIter := bucket.List(&blob.ListOptions{Prefix: "data/"})
			objectCount := 0
			for {
				_, err := listIter.Next(ctx)
				if err != nil {
					if err == io.EOF {
						break
					}
					Expect(err).NotTo(HaveOccurred())
				}
				objectCount += 1
			}
			Expect(objectCount).To(Equal(1000))
		})

		It("should list all files with delimiter", func(ctx context.Context) {
			rsfServer.SetHandler(func(w http.ResponseWriter, r *http.Request, n uint32) {
				Expect(r.Method).To(Equal(http.MethodPost))
				Expect(r.URL.Path).To(Equal("/list"))
				Expect(r.URL.Query().Get("bucket")).To(Equal(bucketName))
				Expect(r.URL.Query().Get("limit")).To(Equal("1000"))
				Expect(r.URL.Query().Get("delimiter")).To(Equal("/"))

				commonPrefixes := make([]string, 0, 1000)
				for i := uint32(0); i < 1000; i++ {
					commonPrefixes = append(commonPrefixes, fmt.Sprintf("data_%05d/", n*1000+i))
				}
				responseBodyJson := map[string]any{"commonPrefixes": commonPrefixes}
				err := json.NewEncoder(w).Encode(responseBodyJson)
				Expect(err).NotTo(HaveOccurred())
			}, 1)
			listIter := bucket.List(&blob.ListOptions{Delimiter: "/"})
			objectCount := 0
			for {
				object, err := listIter.Next(ctx)
				if err != nil {
					if err == io.EOF {
						break
					}
					Expect(err).NotTo(HaveOccurred())
				}
				Expect(object.Key).To(Equal(fmt.Sprintf("data_%05d/", objectCount)))
				Expect(object.IsDir).To(BeTrue())
				objectCount += 1
			}
			Expect(objectCount).To(Equal(1000))
		})
	})

	Context("Attributes", func() {
		It("should get attributes", func(ctx context.Context) {
			ioSrcServer.SetHandler(func(w http.ResponseWriter, r *http.Request, n uint32) {
				Expect(r.Method).To(Equal(http.MethodHead))
				Expect(r.URL.Path).To(Equal("/existed-file"))
				Expect(r.URL.Query().Has("e")).To(BeTrue())
				Expect(r.URL.Query().Has("token")).To(BeTrue())

				w.Header().Set("Content-Type", "text/plain")
				w.Header().Set("Content-Md5", "fakemd5")
				w.Header().Set("Etag", "fakeetag")
				w.Header().Set("Content-Length", "1024")
				w.Header().Set("Last-Modified", time.Now().Format(time.RFC1123))
				w.Header().Set("x-qn-meta-data-a", "value-1")
				w.Header().Set("x-qn-meta-data-b", "value-2")
			}, 1)
			info, err := bucket.Attributes(ctx, "existed-file")
			Expect(err).NotTo(HaveOccurred())
			Expect(info.Size).To(Equal(int64(1024)))
			Expect(info.ContentType).To(Equal("text/plain"))
			Expect(info.MD5).To(Equal([]byte("fakemd5")))
			Expect(info.ETag).To(Equal("fakeetag"))
			Expect(info.ModTime).To(BeTemporally("~", time.Now(), 5*time.Second))
			Expect(info.Metadata["data-a"]).To(Equal("value-1"))
			Expect(info.Metadata["data-b"]).To(Equal("value-2"))
		})

		It("should not get attributes from non-existed object", func(ctx context.Context) {
			ioSrcServer.SetHandler(func(w http.ResponseWriter, r *http.Request, n uint32) {
				Expect(r.Method).To(Equal(http.MethodHead))
				Expect(r.URL.Path).To(Equal("/non-existed"))
				Expect(r.URL.Query().Has("e")).To(BeTrue())
				Expect(r.URL.Query().Has("token")).To(BeTrue())
				w.WriteHeader(http.StatusNotFound)
			}, 1)
			_, err := bucket.Attributes(ctx, "non-existed")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("kodoblob: unexpected status code 404"))
		})
	})

	Context("Download", func() {
		It("should download object", func(ctx context.Context) {
			ioSrcServer.SetHandler(func(w http.ResponseWriter, r *http.Request, n uint32) {
				Expect(r.Method).To(Equal(http.MethodGet))
				Expect(r.URL.Path).To(Equal("/existed-file"))
				Expect(r.URL.Query().Has("e")).To(BeTrue())
				Expect(r.URL.Query().Has("token")).To(BeTrue())

				w.Header().Set("Content-Type", "text/plain")
				w.Header().Set("Content-Length", "1024")
				w.Header().Set("Last-Modified", time.Now().Format(time.RFC1123))

				_, err := io.Copy(w, io.LimitReader(rand.New(rand.NewSource(time.Now().UnixNano())), 1024))
				Expect(err).NotTo(HaveOccurred())
			}, 1)

			reader, err := bucket.NewReader(ctx, "existed-file", nil)
			Expect(err).NotTo(HaveOccurred())
			defer reader.Close()

			Expect(reader.Size()).To(Equal(int64(1024)))
			Expect(reader.ContentType()).To(Equal("text/plain"))
			Expect(reader.ModTime()).To(BeTemporally("~", time.Now(), 5*time.Second))

			n, err := io.Copy(io.Discard, reader)
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(reader.Size()))

			err = reader.Close()
			Expect(err).NotTo(HaveOccurred())
		})

		It("should download object in range", func(ctx context.Context) {
			ioSrcServer.SetHandler(func(w http.ResponseWriter, r *http.Request, n uint32) {
				Expect(r.Method).To(Equal(http.MethodGet))
				Expect(r.URL.Path).To(Equal("/existed-file"))
				Expect(r.URL.Query().Has("e")).To(BeTrue())
				Expect(r.URL.Query().Has("token")).To(BeTrue())

				w.Header().Set("Accept-Ranges", "bytes")
				w.Header().Set("Content-Transfer-Encoding", "binary")
				w.Header().Set("Content-Type", "text/plain")
				w.Header().Set("Content-Length", "2048")
				w.Header().Set("Content-Range", "bytes 1024-3071/1056964608")
				w.Header().Set("Last-Modified", time.Now().Format(time.RFC1123))
				w.WriteHeader(http.StatusPartialContent)

				_, err := io.Copy(w, io.LimitReader(rand.New(rand.NewSource(time.Now().UnixNano())), 2048))
				Expect(err).NotTo(HaveOccurred())
			}, 1)

			reader, err := bucket.NewRangeReader(ctx, "existed-file", 1024, 2*1024, nil)
			Expect(err).NotTo(HaveOccurred())
			defer reader.Close()

			Expect(reader.Size()).To(Equal(int64(2048)))
			Expect(reader.ContentType()).To(Equal("text/plain"))
			Expect(reader.ModTime()).To(BeTemporally("~", time.Now(), 5*time.Second))

			n, err := io.Copy(io.Discard, reader)
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(reader.Size()))

			err = reader.Close()
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Context("Upload", func() {
		It("should upload object", func(ctx context.Context) {
			blocks := [3][]byte{randData(4 * 1024 * 1024), randData(4 * 1024 * 1024), randData(4 * 1024 * 1024)}
			upServer.WithMux(func(mux *http.ServeMux) {
				bucketNameBase64ed := base64.URLEncoding.EncodeToString([]byte("existed-file"))
				pathPrefix := "/buckets/" + bucketName + "/objects/" + bucketNameBase64ed + "/uploads"
				called := uint32(0)
				mux.HandleFunc(pathPrefix, func(w http.ResponseWriter, r *http.Request) {
					n := atomic.AddUint32(&called, 1) - 1
					path := strings.TrimPrefix(r.URL.Path, pathPrefix)
					if path == "" || path == "/" {
						Expect(r.Method).To(Equal(http.MethodPost))
						w.Header().Set("Content-Type", "application/json")
						err := json.NewEncoder(w).Encode(map[string]any{
							"uploadId": "fakeuploadid",
							"expireAt": time.Now().Unix(),
						})
						Expect(err).NotTo(HaveOccurred())
						return
					}
					path = strings.TrimPrefix(path, "/fakeuploadid")
					if path == "" || path == "/" {
						Expect(r.Method).To(Equal(http.MethodPut))
						type UploadPartInfo struct {
							Etag       string `json:"etag"`
							PartNumber int64  `json:"partNumber"`
						}
						var body struct {
							Parts      []UploadPartInfo  `json:"parts"`
							MimeType   string            `json:"mimeType,omitempty"`
							Metadata   map[string]string `json:"metadata,omitempty"`
							CustomVars map[string]string `json:"customVars,omitempty"`
						}
						err := json.NewDecoder(r.Body).Decode(&body)
						Expect(err).NotTo(HaveOccurred())
						Expect(body.MimeType).To(Equal("text/plain"))
						Expect(body.Parts).To(HaveLen(3))
						Expect(body.Parts[0]).To(Equal(UploadPartInfo{Etag: "fakeetag_1", PartNumber: 1}))
						Expect(body.Parts[1]).To(Equal(UploadPartInfo{Etag: "fakeetag_2", PartNumber: 2}))
						Expect(body.Parts[2]).To(Equal(UploadPartInfo{Etag: "fakeetag_3", PartNumber: 3}))
						Expect(body.Metadata["data-a"]).To(Equal("value-1"))
						Expect(body.Metadata["data-b"]).To(Equal("value-2"))
						err = json.NewEncoder(w).Encode(map[string]any{})
						Expect(err).NotTo(HaveOccurred())
						return
					}
					path = strings.TrimPrefix(path, "/")
					Expect(path).To(Equal(strconv.FormatUint(uint64(n), 10)))
					Expect(r.Method).To(Equal(http.MethodPost))

					var buf bytes.Buffer
					contentLength, err := io.Copy(&buf, r.Body)
					Expect(err).NotTo(HaveOccurred())
					Expect(contentLength).To(Equal(int64(len(blocks[n]))))
					Expect(buf.Bytes()).To(Equal(blocks[n]))
					err = json.NewEncoder(w).Encode(map[string]any{"etag": "fakeetag_" + path})
					Expect(err).NotTo(HaveOccurred())
				})
			}, 5)

			writer, err := bucket.NewWriter(ctx, "existed-file", &blob.WriterOptions{
				ContentType: "text/plain",
				Metadata:    map[string]string{"data-a": "value-1", "data-b": "value-2"},
			})
			Expect(err).NotTo(HaveOccurred())
			defer writer.Close()

			for _, block := range blocks {
				n, err := io.Copy(writer, bytes.NewReader(block))
				Expect(err).NotTo(HaveOccurred())
				Expect(n).To(Equal(int64(len(block))))
			}

			err = writer.Close()
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Context("Copy", func() {
		It("should copy object", func(ctx context.Context) {
			rsServer.SetHandler(func(w http.ResponseWriter, r *http.Request, _ uint32) {
				objectNameSrcBase64ed := base64.URLEncoding.EncodeToString([]byte(bucketName + ":src-file"))
				objectNameDstBase64ed := base64.URLEncoding.EncodeToString([]byte(bucketName + ":dst-file"))
				Expect(r.Method).To(Equal(http.MethodPost))
				Expect(r.URL.Path).To(Equal("/copy/" + objectNameSrcBase64ed + "/" + objectNameDstBase64ed + "/force/true"))
			}, 1)
			err := bucket.Copy(ctx, "dst-file", "src-file", nil)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Context("Delte", func() {
		It("should delete object", func(ctx context.Context) {
			rsServer.SetHandler(func(w http.ResponseWriter, r *http.Request, _ uint32) {
				objectNameDstBase64ed := base64.URLEncoding.EncodeToString([]byte(bucketName + ":dst-file"))
				Expect(r.Method).To(Equal(http.MethodPost))
				Expect(r.URL.Path).To(Equal("/delete/" + objectNameDstBase64ed))
			}, 1)

			err := bucket.Delete(ctx, "dst-file")
			Expect(err).NotTo(HaveOccurred())
		})
	})
})
