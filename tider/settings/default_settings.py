"""Priority: `project`(take the higher)"""

BROKER = 'redis'
BROKER_QUEUE = None
BROKER_LIMIT = 10000
BROKER_URL = None
BROKER_CONN_KWARGS = {
    'host': 'localhost',
    'port': 6379,
    'db': 0,
    'encoding': 'utf-8'
}

CONCURRENCY = 1
# crawler settings
CRAWLER_ROLE = 'crawler'  # producer, worker
CUSTOM_DEFAULT_SETTINGS_MODULE = None

DUPEFILTER_CLASS = None
REQUEST_FILTER_JOB_DIR = ""

EXPLORER = "tider.core.explorer.Explorer"
EXPLORER_DESIGNATED_PROXY = None
RETRY_PRIORITY_ADJUST = 1

# Never use this option to select the eventlet or gevent pool.
# You must use the -P option to tider crawl instead,
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

SPIDER = None
SPIDER_CONFIG = {}
SPIDER_SCHEMAS = {}
STATE_DB_URL = ''
STATE_DB_CONNECTION_KWARGS = {}
SPIDER_LOADER_CLASS = "tider.spiderloader.SpiderLoader"

WORKER_TIMEOUT = None
WORKER_CONCURRENCY = 1

DEPTH_LIMIT = 0
DEPTH_PRIORITY = 0

ITEM_PROCESSOR = "tider.pipelines.ItemPipelineManager"
ITEM_PIPELINES = {}


FILE_STORE_SETTINGS = {}

STATS_CLASS = 'tider.statscollector.MemoryStatsCollector'
STATS_DUMP = True
STATSMAILER_RCPTS = []

ALARM_CLASS = 'tider.alarm.Alarm'
ALARM_MESSAGE_TYPE = 'text'  # markdown

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
