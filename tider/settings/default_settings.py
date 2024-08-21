"""Priority: `project`(take the higher)"""

ALARM_CLASS = 'tider.alarm.Alarm'
ALARM_MESSAGE_TYPE = 'text'  # markdown | text | html

ASYNC_PARSE = True

BROKER_TYPE = 'redis'
BROKER_URL = None
BROKER_HOST = 'localhost'
BROKE_PORT = 6379
BROKER_VIRTUAL_HOST = 0
BROKEN_ENCODING = 'utf-8'
BROKER_QUEUE = None
BROKER_LIMIT = 10000
BROKER_VALIDATE_MESSAGE = True

CONCURRENCY = 1
CONTROL_URL = None  # for heartbeat and control

DEPTH_LIMIT = 0
DEPTH_PRIORITY = 0

DUPEFILTER_CLASS = None

EXPLORER = "tider.core.explorer.Explorer"
EXPLORER_DESIGNATED_PROXY = None
EXPLORER_RETRY_PRIORITY_ADJUST = 1

FS_BASEDIR = ""

FTP_USER = ""
FTP_PASSWORD = ""
FTP_USE_ACTIVE_MODE = False
FTP_URI = ""

ITEM_PIPELINES = {}
ITEM_PROCESSOR = "tider.pipelines.ItemPipelineManager"

LOG_ENABLED = True
LOG_ENCODING = "utf-8"
LOG_FORMAT = "%(asctime)s [%(process)d - %(threadName)s - %(name)s(line: %(lineno)d) - %(levelname)s] %(message)s"
LOG_DATEFORMAT = None
LOG_MAX_BYTES = 30 * 1024 * 1024
LOG_BACKUP_COUNT = 2
LOG_STDOUT = False
LOG_LEVEL = "INFO"
LOG_COLORIZE = False
LOG_FILE_ENABLED = False
LOG_DIRECTORY = '/'
LOG_FILE = ''
LOG_FILE_APPEND = True

OSS_ACCESS_KEY_ID = ""
OSS_ACCESS_KEY_SECRET = ""
OSS_BUCKET_NAME = ""
OSS_DEFAULT_TIMEOUT = 50
OSS_DOMAIN = ""
OSS_ENDPOINT = ""

# Never use this option to select the eventlet or gevent pool.
# You must use the -P option to tider worker instead,
# to ensure the monkey patches aren’t applied too late,
# causing things to break in strange ways.
POOL = "threads"
PROXY_SCHEMAS = {}
PROXY_PARAMS = {}

SCHEDULER = "tider.core.scheduler.Scheduler"
SCHEDULER_PRIORITY_QUEUE = "tider.structures.pqueues.PriorityMemoryQueue"
SCHEDULER_URL = None
SCHEDULER_KEY = None
SCHEDULER_CONNECTION_KWARGS = {}

SPIDER_LOADER_CLASS = "tider.spiderloader.SpiderLoader"
SPIDER_LOADER_WARN_ONLY = False

STATE_DB_URL = ''
STATE_DB_CONNECTION_KWARGS = {}

STATS_CLASS = 'tider.statscollector.MemoryStatsCollector'
STATS_DUMP = True
STATSMAILER_RCPTS = []

STORE_SCHEMAS = {}

WGET_OUTPUT_DIR = ""
WORKER_TIMEOUT = None
