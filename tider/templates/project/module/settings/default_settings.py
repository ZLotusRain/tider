import psutil

BROKER_TRANSPORT_OPTIONS = {
    'retry_on_timeout': True  # redis config
}

CONCURRENCY = psutil.cpu_count(logical=False) * 4

DEFAULT_USER_AGENT = None

DOWNLOADER_BROWSER_CACHE_DIR = "/data/tider/BrowserDownloader"
DOWNLOADER_BROWSER_CACHE_STATICS = True
DOWNLOADER_WGET_DIR = "/data/tider/WgetDownloader/"

EXPLORER_RETRY_PRIORITY_ADJUST = 0
EXPLORER_SESSION_VERIFY = False

LOG_STDOUT = True

SPIDER_MODULES = []
SPIDER_SCHEMAS = {}

del psutil
