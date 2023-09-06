# Qiniu gocloud.dev blob driver

七牛 kodoblob 为 [gocloud.dev](https://gocloud.dev/) 提供了驱动，可以通过使用 [blob](https://gocloud.dev/blob) 包对七牛 Bucket 中的 Blob 进行读写，列举或删除。

## 代码案例

### 打开一个七牛 Bucket

```go
package main

import (
	"context"
	"fmt"
	"os"

	_ "github.com/bachue/go-cloud-dev-qiniu-driver/kodoblob"
	"gocloud.dev/blob"
)

func main() {
	bucket, err := blob.OpenBucket(context.Background(), "kodo://<Qiniu Access Key>:<Qiniu Secret Key>@<Qiniu Bucket Name>?useHttps")
	if err != nil {
		fmt.Fprintf(os.Stderr, "could not open bucket: %v\n", err)
		os.Exit(1)
	}
	defer bucket.Close()

    // 对 bucket 进行操作
}
```

这里的 URL 必须遵循以下格式

```
kodo://<Qiniu Access Key>:<Qiniu Secret Key>@<Qiniu Bucket Name>?<Options>
```

其中 `Options` 以 URL 查询的形式设置，支持以下选项：

| 名称 | 值类型 | 备注 |
|---|---|---|
| `useHttps` | 布尔值 | 是否使用 HTTPS 协议，默认不使用 |
| `downloadDomain` | 字符串列表 | 下载域名，如果不配置则使用默认源站域名，可以配置多个下载域名 |
| `signDownloadUrl` | 布尔值 | 是否对下载 URL 签名，对于私有空间来说，这是必须的，默认不签名 |
| `bucketHost` | 字符串列表 | 设置 Bucket 域名，可以配置多个 Bucket 域名，默认使用公有云 Bucket 域名 |
| `srcUpHost` | 字符串列表 | 设置上传源站域名，可以配置多个上传源站域名，默认通过 Bucket 域名查询获取 |
| `cdnUpHost` | 字符串列表 | 设置上传加速域名，可以配置多个上传加速域名，默认通过 Bucket 域名查询获取 |
| `rsHost` | 字符串列表 | 设置 RS 域名，可以配置多个 RS 域名，默认通过 Bucket 域名查询获取 |
| `rsfHost` | 字符串列表 | 设置 RSF 域名，可以配置多个 RSF 域名，默认通过 Bucket 域名查询获取 |
| `apiHost` | 字符串列表 | 设置 API 域名，可以配置多个 API 域名，默认通过 Bucket 域名查询获取 |

### 向七牛 Bucket 写入数据

```go
package main

import (
	"context"
	"fmt"
	"os"

	_ "github.com/bachue/go-cloud-dev-qiniu-driver/kodoblob"
	"gocloud.dev/blob"
)

func main() {
	bucket, err := blob.OpenBucket(context.Background(), "kodo://<Qiniu Access Key>:<Qiniu Secret Key>@<Qiniu Bucket Name>?useHttps")
	if err != nil {
		fmt.Fprintf(os.Stderr, "could not open bucket: %v\n", err)
		os.Exit(1)
	}
	defer bucket.Close()

	w, err := bucket.NewWriter(context.Background(), "<Key>", nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "could not open object for writing: %v\n", err)
		os.Exit(1)
	}
	defer w.Close()

	// 对 w 写入数据
}

```

### 从七牛 Bucket 读取数据

```go
package main

import (
	"context"
	"fmt"
	"os"

	_ "github.com/bachue/go-cloud-dev-qiniu-driver/kodoblob"
	"gocloud.dev/blob"
)

func main() {
	bucket, err := blob.OpenBucket(context.Background(), "kodo://<Qiniu Access Key>:<Qiniu Secret Key>@<Qiniu Bucket Name>?useHttps")
	if err != nil {
		fmt.Fprintf(os.Stderr, "could not open bucket: %v\n", err)
		os.Exit(1)
	}
	defer bucket.Close()

	r, err := bucket.NewReader(context.Background(), "<Key>", nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "could not open object for reading: %v\n", err)
		os.Exit(1)
	}
	defer r.Close()

    // 从 r 读取数据
}
```

### 从七牛 Bucket 读取范围数据

`gocloud.dev/blob` 支持读取指定偏移量的数据。

```go
package main

import (
	"context"
	"fmt"
	"os"

	_ "github.com/bachue/go-cloud-dev-qiniu-driver/kodoblob"
	"gocloud.dev/blob"
)

func main() {
	bucket, err := blob.OpenBucket(context.Background(), "kodo://<Qiniu Access Key>:<Qiniu Secret Key>@<Qiniu Bucket Name>?useHttps")
	if err != nil {
		fmt.Fprintf(os.Stderr, "could not open bucket: %v\n", err)
		os.Exit(1)
	}
	defer bucket.Close()

	r, err := bucket.NewRangeReader(ctx, "<Key>", 1024, 4096, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "could not open object for reading: %v\n", err)
		os.Exit(1)
	}
	defer r.Close()

    // 从 r 读取数据
}
```

### 从七牛 Bucket 删除数据

```go
package main

import (
	"context"
	"fmt"
	"os"

	_ "github.com/bachue/go-cloud-dev-qiniu-driver/kodoblob"
	"gocloud.dev/blob"
)

func main() {
	bucket, err := blob.OpenBucket(context.Background(), "kodo://<Qiniu Access Key>:<Qiniu Secret Key>@<Qiniu Bucket Name>?useHttps")
	if err != nil {
		fmt.Fprintf(os.Stderr, "could not open bucket: %v\n", err)
		os.Exit(1)
	}
	defer bucket.Close()

	if err = bucket.Delete(context.Background(), "1G.65"); err != nil {
		fmt.Fprintf(os.Stderr, "could not delete object: %v\n", err)
		os.Exit(1)
	}
}
```

## 贡献记录

- [所有贡献者](https://github.com/bachue/go-cloud-dev-qiniu-driver/contributors)

## 联系我们

- 如果需要帮助，请提交工单（在portal右侧点击咨询和建议提交工单，或者直接向 support@qiniu.com 发送邮件）
- 如果有什么问题，可以到问答社区提问，[问答社区](http://qiniu.segmentfault.com/)
- 更详细的文档，见[官方文档站](http://developer.qiniu.com/)
- 如果发现了bug， 欢迎提交 [issue](https://github.com/bachue/go-cloud-dev-qiniu-driver/issues)
- 如果有功能需求，欢迎提交 [issue](https://github.com/bachue/go-cloud-dev-qiniu-driver/issues)
- 如果要提交代码，欢迎提交 [pull request](https://github.com/bachue/go-cloud-dev-qiniu-driver/pulls)
- 欢迎关注我们的[微信](http://www.qiniu.com/#weixin) [微博](http://weibo.com/qiniutek)，及时获取动态信息。

## 代码许可

The Apache License v2.0. 详情见 [License 文件](https://github.com/bachue/go-cloud-dev-qiniu-driver/blob/master/LICENSE).
