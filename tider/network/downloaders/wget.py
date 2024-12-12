import shlex
import tempfile
import subprocess
from subprocess import DEVNULL

from tider import Request, Response


class WgetDownloader:

    lazy = True

    def __init__(self, output_dir=None, quiet=True):
        self.wget_cmd = 'wget'
        self.output_dir = output_dir or '/'
        self.quiet = quiet

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        return cls(output_dir=settings.get('DOWNLOADER_WGET_DIR'),
                   quiet=settings.getbool('DOWNLOADER_WGET_QUIET'),)

    def build_response(self, request, resp):
        response = Response(request)
        resp.flush()
        resp.seek(0)
        response.raw = resp
        response.url = request.url
        response.status_code = 200

        return response

    def download_request(self, request: Request):
        ntf = tempfile.NamedTemporaryFile(dir=self.output_dir)
        cmd = self.to_wget(request, output=ntf.name)
        timeout = request.timeout or 60
        timeout = max(timeout, 100)
        request.proxy.connect()
        try:
            subprocess.run(cmd, timeout=timeout, check=True, stdout=DEVNULL, stderr=DEVNULL)
            response = self.build_response(request, ntf)
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
            returncode = getattr(e, 'returncode', None)
            if returncode in (1, 4, 5, 7, 8) or isinstance(e, subprocess.TimeoutExpired):
                request.invalidate_proxy()
            ntf.close()
            response = Response(request)
            response.fail(error=e)
        request.proxy.disconnect()
        return response

    def to_wget(self, request, output):
        wget_cmd = self.wget_cmd
        if self.quiet:
            wget_cmd += ' -q'
        wget_cmd += ' --tries=1'
        if not request.verify:
            wget_cmd += ' --no-check-certificate'

        timeout = request.timeout
        if isinstance(timeout, tuple):
            timeout = timeout[-1]
        wget_cmd += f' --timeout={timeout}'

        method = request.method.upper()
        if method == "POST":
            data = request.body
            wget_cmd += f' --post-data={data}'
        ua = request.headers.get('User-Agent')
        if ua:
            wget_cmd += f' -U "{ua}"'
        if request.proxies.get('http'):
            wget_cmd += f' -e "http_proxy={request.proxies["http"]}"'
        if request.proxies.get('https'):
            wget_cmd += f' -e "https_proxy={request.proxies["https"]}"'
        wget_cmd += f' -O {output} {request.url}'
        return shlex.split(wget_cmd)
