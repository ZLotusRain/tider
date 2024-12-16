---
hide:
  - navigation
---

# Release Notes



## 3.2.0

### Develop&Maintain Lifetime

2024/08/10 - now

### What's changed

#### Features

- 新增`InvalidRequest`异常用于记录`Request`初始化时发生的异常

- 新增`MailSender`及`StatsMailer`用于在爬虫结束后发送状态信息至指定邮箱
- 新增`ExplorerAwarePriorityQueue`，将优先调度限制并发数最小且存在剩余并发额度的请求
- 当`SPIDER_LOADER_WARN_ONLY `设置为True时，同时忽略`SyntaxError`
- 调整默认加载`Spider`的方式，以配置中的`schema`为单位分开存储各`schema`下的`Spider`，不再直接将`schema`和爬虫名拼接
- `Crawler.autodiscover_spiders`将根据是否提供`schema`参数返回爬虫列表或字典
- 新增`Crawler.group`用于将爬虫任务分组，同组的任务为同一种任务
- `Crawler.get_hostname()`同时判断远程和本地是否存在重复任务
- `Crawler.get_hostname()`中使用print输出信息，防止日志模块未配置
- 在`Crawler`关闭时同时释放`pidlock`
- `Engine`中调度请求时，使用`time.sleep`替换`Crawler.maybe_sleep`防止多线程执行过程中线程无法切换
- `Engine`中调度请求时，设置单次调度的最大数量，防止因并发限制等导致长时间调度同一批请求

- `Engine`中调度请求时，新增`polling`状态用于标识正在将请求从调度队列中取出

- 当使用`Cold Shutdown`时同时清除调度队列中的请求
- `Broker`中新增`poller`用于定期调度请求
- 将`AmqpBroker`的默认`prefetch_count`提高到并发量的10倍
- `AmqpBroker`在`socket.timeout`时清除`qos`中的`dirty`对象(已确认消费或已丢弃的消息)
- 将并发池的部分属性修改为实例属性
- `Promise`中新增`NodeState`用于控制节点状态
- `Promise`中的根节点支持调度非`Request`对象
- `Promise`中的`add_child`支持添加非`Request`对象
- `Promise`中新增`spider`参数用于未来和`crawler`直接交互，定量调度
- `Spider`中新增`default_ua`参数用于直接获取全局配置的默认User-Agent
- `Spider`不再支持`process`方法
- `Explorer`中新增对域名或API的并发限制，当前进行的请求中被限制的相关请求达到并发上线后，新的同类型请求将被重新丢进调度队列

- `Explorer`模块新增`clear_idle_conns()`方法用于在请求不饱和的情况下清理不活跃的连接
- `Explorer`模块的`_transport`方法新增参数`transferring`用于记录正在进行的请求，防止请求记录不及时导致程序提前终止
- `Explorer`中使用异步唤醒`worker`时，捕获`queue.Full`异常，防止并发池无法再创建新的`worker`
- `Explorer`中新增`try_explore`用于直接发起请求
- `Session`模块获取响应后如果请求方式为`HEAD`，无论是否使用stream模式，都会读取内容
- 新增`PlaywrightDownloader`和`DrissionPageDownloader`用于模拟浏览器请求
- `Response`中新增`on_consumed`用于在`content`被消费时触发回调，在框架中用于限制`stream`模式下的连接数
- `Response`中新增`check_length`用于检查响应体是否已全部读取
- `Response`若未被消费或失败则直接关闭`urlllib3.HTTPResponse`
- `Response`中将`image/webp`对应到`png`文件类型
- 读取`Response`的内容时支持忽略读取异常
- 从`dummy`创建`Response`时，内容消耗标志位置为True
- 在关闭`Response`时，使用`release_conn`替换`drain_conn`
- 在关闭`Response`时，同时清理从`lxml.etree.from_string()`创建的`selector`的根节点
- 在解析完成后清理`callback`和`errback`
- 在关闭`Explorer`或`Parser`时，捕获关闭时产生的不可控的异常
- `Proxy`不再支持`add_errback`方法，只接受在初始化时指定`on_discard`回调
- 从`ProxyPool`中获取`Proxy`时同时清理无效的`Proxy`
- 从`RecentlyUsedContainer`中获取对象时清理过期对象
- 从连接池获取连接时判断连接是否已过期
- 定期清理过期的`connection`时，不再将其从连接池移除并往连接池放None占位
- `OssStore`中使用`Explorer`替换原本的`requests`逻辑，确保每次操作都完全消耗了响应
- 新增`StoreError`用于处理和文件持久化相关的异常
- `tider crawl`新增`--remove-file-after-consumed`用于指示是否在文件读取完后删除文件
- `tider crawl`新增`--mail-to`用于在程序结束后发送状态邮件
- `tider bench`支持自定义请求链接、代理及解析逻辑
- `tider multi`功能重构，根据`schema`及相关命令行生成对应的爬虫启动节点，在必要时生成临时`Crawler`获取完整的节点名称用于管理进程
- 新增`PidDirectory`用于管理同组进程，在组内无进程时移除相应目录
- `tider multi`新增使用帮助说明
- 新增`tider multi kill/show/stop/stopwait/expand/restart/get/names`
- 当命令行启动失败时，如果提供了日志文件，则错误会记录到日志文件中，如果日志文件中使用了`%n`，那错误日志仍只会记录到组日志中

- `Item`中设置默认值时使用`deepcopy`防止值被异常修改

- `utils.log`中新增`iter_open_logger_fds`
- `utils.network`中新增`cookies_from_str`
- 新增内存占用曲线绘制工具

#### Bugfixes

- 移除`RecentlyUsedContainer.clean_up()`中的锁防止程序在部分情况下卡死
- 在请求重试时只更新`explorer_after`参数而不更新`delay`防止对后续请求产生影响
- 连接不活跃时不再立即清洗连接中的缓存防止连接中无法正确判断连接是否需要保持
- 直接遍历连接池的队列检查连接是否过期而不是挨个取出检查
- 清理过期连接时将过期判断逻辑和移除过期连接并放入None占位逻辑分步处理
- 修复了`Spider.update_settings`没有被正确调用的问题
- 使用命令行方式启动时，在启动`Crawler`之前将部分命令行参数更新到配置中
- 在`Crawler`关闭时直接设置`_spider_closed`事件
- 缩短`Crawler`启动时的`hostname`重复性检测时间，防止使用`multi start`启动多进程时出现重复节点
- `Response.iter_content`中不再捕获`BaseException`防止出现`Exception ignored`
- fix `ResourceWarning` on CONNECT with Python < 3.11.4
- 修复了`AmqpBroker`中使用`redis`时，直接使用`client`时仍启动`multi`的问题
- 提取`tider multi`中的`-s`参数防止未发现指定爬虫导致操作全部爬虫
- 使用`tider multi restart`启动多个进程时，判断各个爬虫启动节点的name是否一致，不再处理name重复的节点，防止进程重启后进程组下一直存在进程文件导致进程终止判断处卡死
- 使用`tider multi`时，不再自动创建爬虫组目录，只有在创建`Pidfile`时才会自动创建不存在的目录
- 在使用`os.fork()`创建子进程后记录子进程pid并根据是否允许重复任务来尝试移除对应的`pidfile`

#### Internal

- 升级部分依赖的最低要求版本防止内存泄漏
  - `lxml`: 4.9.3 -> 5.3.0
  - `openpyxl`: 3.1.0 -> 3.1.5 
- 其他依赖最低版本升级
  - `curl_cffi`:  0.6.3b1-> 0.7.3

## 3.1.0

### Develop&Maintain Lifetime

2024/07/10 - 2024/08/10

### What's changed

#### Features

- 使用后台启动时规范化爬虫任务的唯一标识符(`hostname`)
- 统一参数名，修改日志模块的`nodename`参数为`hostname`
- `crawl`命令新增`data-source`参数用于从本地或网络文件中获取消息来构建起始请求
- 新增`FilesBroker`用于从本地或网络文件中获取消息生成起始请求
- `ImpersonateDownloader`新增Python版本检测用于提供不同的请求参数
- `Request`初始化时优先移除`url`中的空格符
- 使用`ping`替换`sames`来判断是否有重复的任务正在执行
- 设置默认的代理过期时间为60s
- 只有在请求连接池满的时候才设置连接池为不活跃状态
- 更新了`stats`的获取方法，将`svr`设置为`cached_property`
- 在使用`alioss`管理文件时在每次操作后及时关闭响应连接
- 使用`iter_content`读取`response`时若发生异常则将`response`标记为`failed`
- `Request`新增默认忽略412状态码
- 使用`iter_generator`时，确保在任何情况下都正常关闭了迭代器
- `iter_generator`支持自定义`sleep`方法
- 新增`RSA`加密工具
- 新增`saferepr`、`noop`工具函数
- `host_format`中新增`%i`和`%I`的格式化
- `platforms`模块中新增`signal_name`方法用于将信号数字转换为名称
- 当关闭`response`时，如果发现请求失败则关闭连接
- 在调度请求过程中判断系统是否终止，防止终止后还有请求调度
- 在使用`Cold Shutdown`时强制清理`explorer`及`parser`队列中的数据
- 在`engine`及`explorer`中捕获`BaseException`防止系统错误丢失
- `parser`未处于running状态下不再处理爬虫输出
- `parser`中在处理解析响应过程中判断系统是否终止
- 移除了主循环中的`gc.collect()`提升性能
- 在需要时关闭`start_requests`迭代器
- 设置`delay`的`Request`对象的meta属性中新增`explore_after`参数用于判断何时处理
- `explorer`中对设置`delay`的请求进行判断，将不满足条件的请求丢回队列末尾

#### Bugfixes

- 不再使用`response.url`作为`filetype`的辅助判断防止出现文件类型异常
- 修复了`Session`中加载下载器异常后日志中参数对应错误的问题
- 使用`copy_with`产生新请求时先将headers中的cookie移出防止cookie覆盖失败
- 修复了`response.retry()`时`meta`无法更新的问题
- `HTTPDownloader`中修减多余的引导路径分隔符(trim excess leading path separators)
- 避免在所有情况下都重新导致`SSL`证书来提高并发性能
- 修复了`response.clean_text()`中移除非空div标签的问题
- 修复了代理在有效性检测时因过期时间被其他线程设置为了None导致类型异常的问题
- 在`nodesplit`中使用`rsplit `防止爬虫名中含有分隔符
- 修复了在`AmqpBroker`恢复未确认数据时因pipe执行顺序导致的`redis.WatchError`的问题
- 修复了使用`Ctrl+C`不能正常关闭任务的问题
- 修复了因`DummyBroker`调度完会设置`spider_closed`导致在`on_message_consuemd`中其余请求不被调度的问题

#### Deprecations

- Tider has officially dropped support for Python lower than 3.7

## 3.0.1(2024-05-28)

### Develop&Maintain Lifetime

2024/05/21 - 2024/07/10

### What's changed

#### Features

- `response.clean_text()`方法中移除更多无效的HTML标签

- `response.filetype`使用`response.url`作为辅助判断
- `AmqpBroker`的`redis`队列支持处理非`json`格式的数据

#### Bugfixes

- 移除了请求头中值为`None`的头

- 修复了在判断`response.filetype`时因通过`mimetypes`将`application/ms-download`绑定到PDF类型导致在使用oss上传PDF文件时默认将请求头`Content-type`设置为`application/ms-download`的问题

## 3.0.0(2024-05-20)

### Develop&Maintain Lifetime

2023/09/04 - 2024/05/28

### What's changed

#### Structures

- `tider.core` ->` tider.crawler`

- `tider.dupfilters` ->` tider.dupefilters`(keep the same as `scrapy`)

#### Features

- 重新设计`Tider`模块用于管理`Crawler`，重新设计`Crawler`模块用于启动爬虫任务
- 移除了文件存储管理模块，新增`Extensions`模块用于管理其他工具
- 重新构造爬虫任务的节点名称，区分不同类型的爬虫
- 新增`Logging`模块管理配置日志
- 命令行方式支持输出带颜色的日志
- 新增`signals`模块用于收发及处理各种信号(from celery)
- 默认使用动态分配并发资源的方式进行请求和解析
- 拆分`Explorer`模块的各项功能，新增`Session`模块用于管理实际的请求模块
- 新增`HTTPDownloader`，默认使用该模块进行实际的请求行为，支持HTTP/2，新增`DEFAULT_BLOCKSIZE`参数
- 新增`WgetDownloader`用于使用`wget`命令进行请求
- 新增`ImpersonateDownloader`用于使用模拟指纹请求
- 现在用户可以新增自定义请求模块处理特殊的请求
- 重构`Proxy`，以便支持在`HTTPDownloader`中同步代理失效信息
- 现在将会在没有请求使用且代理失效时丢弃代理
- `Parser`模块支持同时处理多个`Item`
- 新增`BrokersManager`用于统一管理`Broker`，从队列中构造起始请求，并协助调度请求
- 新增`AmqpBroker`用于从amqp队列中获取消息(详见`kombu`支持的队列)，并支持连接失败重试、redis集群及消息确认机制([celery#8672](https://github.com/celery/celery/issues/8672))
- 新增`DummyBroker`用于直接从`spider.start_requests`中调度请求
- `Scheduler`中默认使用由下游队列组成的优先级队列来调度请求
- 新增`SCHEDULER_BODY_PRIORITY_ADJUST`，用于在调度请求时自动提高body较大的请求的优先级
- 新增`state`模块用于控制运行状态
- 使用`response.follow`在请求链中保持cookies
- 使用`request.session_cookies`保存请求流程中产生的cookies
- 重构`SpiderLoader`，只从`SPIDER_MODULES`和`SPIDER_SCHEMAS`配置中探测爬虫
- `PromiseNode`中新增`source`属性用于存储根节点初始化时携带的请求并在适当时调度

#### Bugfixes

- 在主循环中加入等待机制，修复了因协程无法切换导致的程序卡住的问题
- 在创建并发池时，不再统一使用`crawler.app.Pool`，而是分开创建，防止类属性错乱
- 根据并发调整并发池大小防止在使用`threads`并发池提交任务时卡住
- 修复了使用线程作为并发池时，在程序结束时`_work_queue`因队列大小不够导致无限等待的问题
- 修复了在`response.check_error`时因丢弃了`error`导致的资源泄漏问题
- 修复了因在配置日志之前有其他模块直接使用了logging输出日志导致配置日志时检测到logging已被配置过的问题
- 在使用1.26.15以上版本`urllib3`时，在使用`gevent`并发池时，修改`urllib3`的连接池，防止程序卡死 ([urllib3#3289](https://github.com/urllib3/urllib3/issues/3289))
- 在`Promise`初始化时优先初始化`values`属性，防止在添加子节点时`values`缺失
- promise嵌套问题修复
  - 子promise的节点及时用父promise的values更新自己的values
  - 有parent的promise在进行resolve时新产生的请求应链接到parent上
- 在`PromiseNode`的`source`调度完成后立即调度新产生的子节点

## 2.0.0(2022-11-02)

### Develop&Maintain Lifetime

2022/05/16 - 2023/09/04

### What's changed

#### Features

- 不再使用`Crawler`和`CrawlerProcess`，而是使用`Tider`直接管理爬虫
- 新增Remote Control/Inspect功能
- 新增`sse`命令用于查看运行时的`settings`、`stats`、`engine`
- 新增运行期间的队列负载检测避免队列中存在大量未处理的请求或响应
- 使用`signal.signal(signal.SIGALRM, _)`方法处理请求模块长时间未响应的问题
- 在`Spider`中使用代理的`Tider`对象
- 新增`Session`模块负责底层请求实现
- 支持HTTP/2
- 借助`curl_cffi`实现对模拟指纹请求的支持
- 通过`response.retry`重试时默认重置代理
- 新增代理池管理及代理对象管理
- 新增`ASYNC_PARSE`配置项用于控制`Parser`模块是否和`Explorer`模块异步执行
- 新增报警模块，支持配置自动报警
- 新增文件存储管理模块用于持久化保存文件
- 支持处理从`start_requests`中产生的`Item`
- 使用`click`构建新的命令行启动方式，并支持通过参数配置实现后台启动

#### Bugfixes

- 在使用`response.iter_content`时使用列表暂存并拼接`chunk`而不是直接将每次迭代产生的`chunk`合并，以避免无法切换协程
- 使用wget请求时如果未从`Request`对象的参数中获取到`timeout`，则设置默认的超时时间，防止长时间等待
- 修复了使用命令行启动时因初始化参数对应错误导致的启动失败的问题
- 修复了`Control`模块中因连接未复用导致的控制失败的问题
- 如果`Item`的字段信息来自`__slots__`，则使字段的默认值为None
- 修复了子`promise`添加到父`promise`后因未使用`request.copy`导致`cb_kwargs`丢失的问题
- 修复了`promise`初始化时在判断`reqs`参数是否有效时提前消耗了`generator`的问题
- 修复了使用`consul`时未关闭`session`导致的`ResourceWarning`

## 1.0.0(2022-05-16)

### Develop&Maintain Lifetime

2021/12/30 - 2022/11/02

## Draft(2021-12-30)

### Develop&Maintain Lifetime

2021/12/28 - 2022/05/16

## RainDrop(history name)(2020-06-12)









