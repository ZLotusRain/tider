# Tider

Tider 是一个 Python 网络爬虫框架，用于采集网页、提取结构化数据和运行采集任务。

它支持以 `Spider` 组织请求与解析回调，也提供同步请求接口。可以从一个爬虫文件开始，
随着需求增加，将下载器、中间件和数据处理流程拆分为可配置组件，并通过消息队列扩展到分布式运行。

![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue)

[Source](https://github.com/ZLotusRain/tider) · [Issues](https://github.com/ZLotusRain/tider/issues) · [Changelog](https://github.com/ZLotusRain/tider/blob/main/CHANGELOG.md)

## Installation

需要 **Python 3.11+**。支持 Linux、macOS 和 Windows；进程管理及可选下载器的可用性取决于操作系统和外部依赖。

安装发布版本：

```console
python -m pip install tider
```

也可以从 Git 安装开发版本：

```console
python -m pip install "tider @ git+https://github.com/ZLotusRain/tider.git"
```

单文件爬虫可以直接运行，使用消息队列任务时再配置对应的 Broker 服务。
可选下载器和代理依赖见下方 [Optional dependencies](#optional-dependencies)。

## Quick start

将下面的代码保存为 `quotes_spider.py`：

```python
from tider.spiders import Spider


class QuotesSpider(Spider):
    name = "quotes"
    start_urls = ["https://quotes.toscrape.com/"]

    def parse(self, response):
        for quote in response.css("div.quote"):
            text = quote.css("span.text::text").get()
            author = quote.css("small.author::text").get()
            self.logger.info("%s — %s", author, text)
            yield {"text": text, "author": author}
```

运行爬虫：

```console
tider runspider --filename quotes_spider.py
```

也可以使用 `python -m tider runspider --filename quotes_spider.py`。
示例读取演示站点首页并打印提取结果；若需要保存结果，可配置项目的 Item Pipeline。
请求、响应和解析回调的调度与清理由框架管理。

## Features

- **采集流程**：使用 `Spider`、`Request` 和 `Response` 组织请求与解析，通过 `Promise` 协调关联请求。
- **数据提取**：提供 CSS、XPath、JSON 和 Beautiful Soup 等解析接口。
- **网络能力**：支持 HTTP/2、流式响应、重试、重定向和代理管理，可按需启用 curl-cffi 或 Playwright 下载器。
- **并发执行**：提供 `threads` 和 `gevent` 并发池，默认使用 `threads`。
- **组件扩展**：可配置 Downloader、Middleware、Scheduler、Broker 和 Pipeline。
- **分布式任务**：通过 Broker 接入消息队列，支持任务分发、消费和消息确认。
- **远程监控**：通过 `status` 和 `inspect` 查询在线节点、运行统计、引擎状态，以及正在传输的请求和正在解析的响应。
- **远程控制与节点管理**：通过 `control` 向指定节点或多个节点发送控制命令，通过 `multi` 启动、停止和重启多个爬虫节点。
- **运行统计与日志**：支持自定义统计指标、文件日志和彩色日志，并可安装相应依赖，通过 OTLP/gRPC 上报日志。
- **报警通知**：提供可扩展的报警接口和企业微信机器人通知实现，可附带爬虫、主机及进程信息。
- **文件存储**：提供本地文件系统、FTP 和阿里云 OSS 存储组件，可在采集流程中按需配置。

远程监控和控制需要配置 `CONTROL_URL`；报警、日志上报及外部存储也需要相应的项目配置。

## Project layout

单文件示例适合快速验证。需要管理多个爬虫和配置时，可以生成项目模板：

```console
tider startproject --project-name myproject
```

生成的主要目录如下：

```text
myproject/
└── myproject/
    ├── spiders/
    ├── settings/
    │   └── default_settings.py
    ├── middlewares/
    ├── pipelines/
    ├── downloaders/
    ├── brokers/
    └── items/
```

将快速上手中的示例保存为 `myproject/myproject/spiders/quotes.py`，并在
`myproject/myproject/settings/default_settings.py` 中设置：

```python
SPIDER_MODULES = ["myproject.spiders"]
```

进入项目目录后运行：

```console
cd myproject
tider --src myproject -s quotes crawl
```

Middleware 用于调整请求和响应处理，Pipeline 用于处理或保存提取结果。

## Optional dependencies

基础安装之外，可以按需添加以下功能：

| Extra | 用途 | 安装命令 |
| --- | --- | --- |
| `curlcffi` | curl-cffi 下载器 | `python -m pip install "tider[curlcffi]"` |
| `playwright` | 浏览器下载器 | `python -m pip install "tider[playwright]"` |
| `socks` | SOCKS 代理 | `python -m pip install "tider[socks]"` |
| `rarfile` | RAR 文件处理 | `python -m pip install "tider[rarfile]"` |

安装 extra 后，还需要在采集项目中选择对应组件。使用 Playwright 前，先安装所需浏览器，例如 Chromium：

```console
python -m playwright install chromium
```

多个 extra 可以组合安装，例如 `python -m pip install "tider[curlcffi,socks]"`。
RAR 解压可能还需要额外的系统工具。

## Development

```console
git clone https://github.com/ZLotusRain/tider.git
cd tider
python -m venv .venv
# macOS / Linux
source .venv/bin/activate
python -m pip install -e .
```

Windows 可使用 `.venv\Scripts\activate` 激活环境。
项目元数据、依赖和打包配置维护在 `pyproject.toml` 中；版本号读取自 `tider/VERSION`。

## Help and support

查看命令及参数：

```console
tider --help
tider runspider --help
tider crawl --help
tider startproject --help
tider status --help
tider inspect --help
tider control --help
tider multi --help
```

版本变化见 [CHANGELOG](https://github.com/ZLotusRain/tider/blob/main/CHANGELOG.md)。
报告问题或提出功能建议，请使用 [GitHub Issues](https://github.com/ZLotusRain/tider/issues)，
并附上 Tider/Python 版本、操作系统、复现步骤及相关异常信息。

## Why named after Tider

从网络中获取数据的过程就像潮涨潮落，潮起潮落后岸边留下了我们想要的“数据”。潮水时而温润如玉，时而凶猛无常，
每一位想从网络的海洋中获取数据的用户都是掌握潮汐的人。如果控制不当，既可能连岸边也无法触及，也可能会使海岸决堤，
我们需要仔细斟酌如何掌握这股潮汐之力。

## Licensing

Copyright (c) 2020-2026 LotusRain.

Tider 社区版本按 **GPL-3.0-or-later** 授权，允许在遵守 GPL 条款的前提下商用、修改和再分发。
协议全文见 [LICENSE](https://github.com/ZLotusRain/tider/blob/main/LICENSE)。

独立商业授权需求可联系 **1161525789@qq.com**，具体范围以书面约定和实际权属为准；此联系渠道不构成另一份许可，也不额外限制 GPL 用户。
著作权不因社区授权而转让，GPL 已授予的专利许可继续按其条款生效。
第三方代码和数据保留各自的版权及许可，相关声明统一附于同一 `LICENSE` 文件末尾。
