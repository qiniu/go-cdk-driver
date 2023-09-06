package kodoblob

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/qiniu/go-sdk/v7/auth"
	"github.com/qiniu/go-sdk/v7/storage"
	"gocloud.dev/blob"
	"gocloud.dev/blob/driver"
	"gocloud.dev/gcerrors"
)

// Scheme is the URL scheme s3blob registers its URLOpener under on
// blob.DefaultMux.
const Scheme = "kodo"

const version = "0.1.0"

var (
	ErrNoAccessKey                 = errors.New("no accessKey provided")
	ErrNoSecretKey                 = errors.New("no secretKey provided")
	ErrNoDownloadDomain            = errors.New("no downloadDomain provided")
	ErrNotSupportedSignedPutUrl    = errors.New("kodoblob: does not support SignedURL for PUT")
	ErrNotSupportedSignedDeleteUrl = errors.New("kodoblob: does not support SignedURL for DELETE")

	appName   = fmt.Sprintf("QiniuGoCloudDevKodoBlob/%s", version)
	userAgent = fmt.Sprintf("%s (%s; %s; %s) %s", appName, runtime.GOOS, runtime.GOARCH, runtime.Compiler, runtime.Version())
)

func init() {
	blob.DefaultURLMux().RegisterBucket(Scheme, new(urlSessionOpener))
	storage.SetAppName(appName)
}

type urlSessionOpener struct{}

func (o *urlSessionOpener) OpenBucketURL(ctx context.Context, u *url.URL) (*blob.Bucket, error) {
	credentials, err := o.createCredentials(u.User)
	if err != nil {
		return nil, err
	}
	config, err := o.createConfig(u.Query())
	if err != nil {
		return nil, err
	}
	downloadDomains, err := o.createDownloadDomains(u.Query())
	if err != nil {
		return nil, err
	}
	_, signDownloadUrl := u.Query()["signDownloadUrl"]
	_, preferHttps := u.Query()["useHttps"]
	uploadConfig := storage.UploadConfig{
		UseHTTPS:      preferHttps,
		UseCdnDomains: true,
	}
	return blob.NewBucket(&bucket{
		name:                u.Host,
		downloadDomains:     downloadDomains,
		credentials:         credentials,
		config:              config,
		willSignDownloadUrl: signDownloadUrl,
		preferHttps:         preferHttps,
		bucketManager:       storage.NewBucketManager(credentials, config),
		uploadManager:       storage.NewUploadManager(&uploadConfig),
	}), nil
}

func (o *urlSessionOpener) createCredentials(userInfo *url.Userinfo) (*auth.Credentials, error) {
	accessKey := userInfo.Username()
	if accessKey == "" {
		return nil, ErrNoAccessKey
	}

	secretKey, secretKeySet := userInfo.Password()
	if secretKeySet && secretKey == "" {
		return nil, ErrNoSecretKey
	}

	return &auth.Credentials{
		AccessKey: accessKey,
		SecretKey: []byte(secretKey),
	}, nil
}

func (o *urlSessionOpener) createConfig(query url.Values) (*storage.Config, error) {
	var (
		config    = &storage.Config{UseCdnDomains: true}
		region    *storage.Region
		useRegion = false
	)
	if ucHosts, ok := query["bucketHost"]; ok {
		storage.SetUcHosts(ucHosts...)
	} else if ucHosts, ok = query["ucHost"]; ok {
		storage.SetUcHosts(ucHosts...)
	}
	_, config.UseHTTPS = query["useHttps"]
	if srcUpHosts, ok := query["srcUpHost"]; ok {
		region.SrcUpHosts = srcUpHosts
		useRegion = true
	}
	if cdnUpHosts, ok := query["cdnUpHost"]; ok {
		region.CdnUpHosts = cdnUpHosts
		useRegion = true
	}
	if rsHost := query.Get("rsHost"); rsHost != "" {
		region.RsHost = rsHost
		useRegion = true
	}
	if rsfHost := query.Get("rsfHost"); rsfHost != "" {
		region.RsfHost = rsfHost
		useRegion = true
	}
	if apiHost := query.Get("apiHost"); apiHost != "" {
		region.ApiHost = apiHost
		useRegion = true
	}
	if useRegion {
		config.Region = region
	}
	return config, nil
}

func (o *urlSessionOpener) createDownloadDomains(query url.Values) ([]*url.URL, error) {
	var downloadUrls []*url.URL

	_, useHttps := query["useHttps"]
	if downloadDomains, ok := query["downloadDomain"]; ok {
		downloadUrls = make([]*url.URL, 0, len(downloadDomains))
		for _, downloadDomain := range downloadDomains {
			if !strings.HasPrefix(downloadDomain, "http://") && !strings.HasPrefix(downloadDomain, "https://") {
				if useHttps {
					downloadDomain = "https://" + downloadDomain
				} else {
					downloadDomain = "http://" + downloadDomain
				}
			}
			if downloadUrl, err := url.Parse(downloadDomain); err != nil {
				return nil, err
			} else {
				downloadUrls = append(downloadUrls, downloadUrl)
			}
		}
	}
	return downloadUrls, nil
}

type bucket struct {
	name                string
	downloadDomains     []*url.URL
	willSignDownloadUrl bool
	preferHttps         bool
	credentials         *auth.Credentials
	config              *storage.Config
	uploadManager       *storage.UploadManager
	bucketManager       *storage.BucketManager
}

func (b *bucket) Close() error {
	return nil
}

func (b *bucket) ErrorCode(err error) gcerrors.ErrorCode {
	// TODO
	return gcerrors.Unknown
}

const defaultPageSize = 1000

func (b *bucket) ListPaged(ctx context.Context, opts *driver.ListOptions) (*driver.ListPage, error) {
	var (
		pageSize         int
		listInputOptions = make([]storage.ListInputOption, 0, 4)
		listResult       driver.ListPage
	)

	if opts != nil {
		if opts.Prefix != "" {
			listInputOptions = append(listInputOptions, storage.ListInputOptionsPrefix(opts.Prefix))
		}
		if opts.Delimiter != "" {
			listInputOptions = append(listInputOptions, storage.ListInputOptionsDelimiter(opts.Delimiter))
		}
		if len(opts.PageToken) > 0 {
			listInputOptions = append(listInputOptions, storage.ListInputOptionsMarker(string(opts.PageToken)))
		}
		pageSize = opts.PageSize
	}

	if pageSize == 0 {
		pageSize = defaultPageSize
	}
	listInputOptions = append(listInputOptions, storage.ListInputOptionsLimit(pageSize))

	if listFilesRet, hasNext, err := b.bucketManager.ListFilesWithContext(ctx, b.name, listInputOptions...); err != nil {
		return nil, err
	} else {
		if hasNext {
			listResult.NextPageToken = []byte(listFilesRet.Marker)
		}
		listResult.Objects = make([]*driver.ListObject, 0, len(listFilesRet.CommonPrefixes)+len(listFilesRet.Items))
		for _, commonPrefix := range listFilesRet.CommonPrefixes {
			listResult.Objects = append(listResult.Objects, &driver.ListObject{
				Key:   commonPrefix,
				IsDir: true,
			})
		}
		for _, item := range listFilesRet.Items {
			listResult.Objects = append(listResult.Objects, &driver.ListObject{
				Key:     item.Key,
				ModTime: time.UnixMicro(item.PutTime * 10),
				Size:    item.Fsize,
				MD5:     []byte(item.Md5),
				IsDir:   false,
			})
		}
		if len(listResult.Objects) > 0 {
			sort.Slice(listResult.Objects, func(i, j int) bool {
				return listResult.Objects[i].Key < listResult.Objects[j].Key
			})
		}
	}

	return &listResult, nil
}

func (b *bucket) As(i interface{}) bool {
	return false
}

func (b *bucket) ErrorAs(err error, i interface{}) bool {
	return errors.As(err, i)
}

type ErrStatusCode struct{ code int }

func (err ErrStatusCode) Error() string {
	return fmt.Sprintf("kodoblob: unexpected status code %d", err.code)
}

func (b *bucket) createDownloadRequest(ctx context.Context, method, key, byteRange string, expiry time.Duration) (*http.Request, error) {
	if downloadUrl, err := b.signDownloadUrl(key, expiry); err != nil {
		return nil, err
	} else if request, err := http.NewRequestWithContext(ctx, method, downloadUrl, http.NoBody); err != nil {
		return nil, err
	} else {
		request.Header.Set("User-Agent", userAgent)
		if byteRange != "" {
			request.Header.Set("Range", byteRange)
		}
		return request, nil
	}
}

func (b *bucket) Attributes(ctx context.Context, key string) (*driver.Attributes, error) {
	if request, err := b.createDownloadRequest(ctx, http.MethodHead, key, "", 3*time.Minute); err != nil {
		return nil, err
	} else if response, err := http.DefaultClient.Do(request); err != nil {
		return nil, err
	} else if err = response.Body.Close(); err != nil {
		return nil, err
	} else {
		if response.StatusCode != http.StatusOK {
			return nil, ErrStatusCode{code: response.StatusCode}
		}
		return b.attributes(response)
	}
}

func (b *bucket) attributes(response *http.Response) (*driver.Attributes, error) {
	headers := response.Header
	attributes := driver.Attributes{
		CacheControl:       headers.Get("Cache-Control"),
		ContentDisposition: headers.Get("Content-Disposition"),
		ContentEncoding:    headers.Get("Content-Encoding"),
		ContentLanguage:    headers.Get("Content-Language"),
		ContentType:        headers.Get("Content-Type"),
		ETag:               headers.Get("Etag"),
		MD5:                []byte(headers.Get("Content-Md5")),
		Size:               response.ContentLength,
		Metadata:           make(map[string]string),
	}
	if lastModified := headers.Get("Last-Modified"); lastModified != "" {
		if t, e := time.Parse(time.RFC1123, lastModified); e == nil {
			attributes.ModTime = t
		}
	}
	for k, v := range headers {
		k = strings.ToLower(k)
		if len(v) > 0 && strings.HasPrefix(k, "x-qn-meta-") {
			k = strings.TrimPrefix(k, "x-qn-meta-")
			attributes.Metadata[k] = v[0]
		}
	}
	return &attributes, nil
}

type reader struct {
	attributes *driver.Attributes
	body       io.ReadCloser
}

func (r reader) As(interface{}) bool {
	return false
}

func (r reader) Read(p []byte) (n int, err error) {
	return r.body.Read(p)
}

func (r reader) Close() error {
	return r.body.Close()
}

func (r reader) Attributes() *driver.ReaderAttributes {
	return &driver.ReaderAttributes{
		ContentType: r.attributes.ContentType,
		ModTime:     r.attributes.ModTime,
		Size:        r.attributes.Size,
	}
}

func (b *bucket) NewRangeReader(ctx context.Context, key string, offset, length int64, opts *driver.ReaderOptions) (driver.Reader, error) {
	var byteRange string

	if offset > 0 && length < 0 {
		byteRange = fmt.Sprintf("bytes=%d-", offset)
	} else if length == 0 {
		byteRange = fmt.Sprintf("bytes=%d-%d", offset, offset)
	} else if length >= 0 {
		byteRange = fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)
	}

	request, err := b.createDownloadRequest(ctx, http.MethodGet, key, byteRange, 3*time.Minute)
	if err != nil {
		return nil, err
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, err
	}
	if length == 0 {
		response.Body.Close()
		response.Body = http.NoBody
	}
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusPartialContent {
		return nil, ErrStatusCode{code: response.StatusCode}
	}
	attributes, err := b.attributes(response)
	if err != nil {
		return nil, err
	}

	return reader{attributes: attributes, body: response.Body}, nil
}

type writer struct {
	b           *bucket
	key         string
	ctx         context.Context
	putPolicy   storage.PutPolicy
	uploadExtra storage.UploadExtra
	source      storage.UploadSource
	wg          sync.WaitGroup
	err         error
	pw          io.WriteCloser
}

func (w *writer) Close() error {
	if w.pw != nil {
		err := w.pw.Close()
		if err == nil {
			err = w.err
		}
		w.err = nil
		w.source = nil
		w.pw = nil
		w.wg.Wait()
		return err
	} else {
		return nil
	}
}

func (w *writer) Write(p []byte) (int, error) {
	if w.pw == nil {
		var err error
		pr, pw := io.Pipe()
		w.pw = pw
		if w.source, err = storage.NewUploadSourceReader(pr, -1); err != nil {
			return 0, err
		}
		w.wg.Add(1)
		go func() {
			defer w.wg.Done()

			var ret storage.UploadRet
			w.err = w.b.uploadManager.Put(w.ctx, &ret, w.putPolicy.UploadToken(w.b.credentials), &w.key, w.source, &w.uploadExtra)
			if w.pw != nil {
				w.source = nil
				w.pw.Close()
				w.pw = nil
			} else {
				w.err = nil
			}
		}()
	}
	if w.err != nil {
		return 0, w.err
	}
	return w.pw.Write(p)
}

func (w *writer) Upload(r io.Reader) error {
	if w.pw != nil {
		_, err := io.Copy(w.pw, r)
		if err != nil {
			return err
		}
		return w.Close()
	} else {
		r, err := storage.NewUploadSourceReader(r, -1)
		if err != nil {
			return err
		}
		var ret storage.UploadRet
		return w.b.uploadManager.Put(w.ctx, &ret, w.putPolicy.UploadToken(w.b.credentials), &w.key, r, &w.uploadExtra)
	}
}

func (b *bucket) NewTypedWriter(ctx context.Context, key string, contentType string, opts *driver.WriterOptions) (driver.Writer, error) {
	putPolicy := storage.PutPolicy{
		Scope:   fmt.Sprintf("%s:%s", b.name, key),
		Expires: 24 * 3600,
	}
	uploadExtra := storage.UploadExtra{
		Params:   convertMetadataToParams(opts.Metadata),
		MimeType: contentType,
	}
	return &writer{
		b:           b,
		key:         key,
		ctx:         ctx,
		putPolicy:   putPolicy,
		uploadExtra: uploadExtra,
		source:      nil,
		err:         nil,
		pw:          nil,
	}, nil
}

func (b *bucket) Copy(ctx context.Context, dstKey, srcKey string, opts *driver.CopyOptions) error {
	// TODO: direct ctx supported
	c := make(chan error)
	go func() {
		c <- b.bucketManager.Copy(b.name, srcKey, b.name, dstKey, true)
	}()
	select {
	case err := <-c:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *bucket) Delete(ctx context.Context, key string) error {
	// TODO: direct ctx supported
	c := make(chan error)
	go func() {
		c <- b.bucketManager.Delete(b.name, key)
	}()
	select {
	case err := <-c:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *bucket) SignedURL(ctx context.Context, key string, opts *driver.SignedURLOptions) (string, error) {
	switch opts.Method {
	case http.MethodGet:
		return b.signDownloadUrl(key, opts.Expiry)
	case http.MethodPut:
		return "", ErrNotSupportedSignedPutUrl
	case http.MethodDelete:
		return "", ErrNotSupportedSignedDeleteUrl
	default:
		return "", fmt.Errorf("unsupported Method %q", opts.Method)
	}
}

func (b *bucket) signDownloadUrl(key string, expiry time.Duration) (string, error) {
	var (
		downloadUrl *url.URL
		signUrl     = b.willSignDownloadUrl
	)
	if len(b.downloadDomains) > 0 {
		downloadUrl = b.downloadDomains[0]
	} else {
		region, err := storage.GetRegionWithOptions(b.credentials.AccessKey, b.name, storage.UCApiOptions{UseHttps: b.preferHttps})
		if err != nil {
			return "", err
		}
		ioSrcHost := region.IoSrcHost
		if ioSrcHost == "" {
			return "", ErrNoDownloadDomain
		}
		signUrl = true
		if !strings.HasPrefix(ioSrcHost, "http://") && !strings.HasPrefix(ioSrcHost, "https://") {
			if b.preferHttps {
				ioSrcHost = "https://" + ioSrcHost
			} else {
				ioSrcHost = "http://" + ioSrcHost
			}
		}
		downloadUrl, err = url.Parse(ioSrcHost)
		if err != nil {
			return "", err
		}
	}
	if signUrl {
		return storage.MakePrivateURLv2(b.credentials, downloadUrl.String(), key, time.Now().Add(expiry).Unix()), nil
	} else {
		return storage.MakePublicURLv2(downloadUrl.String(), key), nil
	}
}

func convertMetadataToParams(metadata map[string]string) map[string]string {
	params := make(map[string]string, len(metadata))
	for k, v := range metadata {
		params["x-qn-meta-"+k] = v
	}
	return params
}
