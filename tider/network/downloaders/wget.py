import tempfile
import subprocess
from subprocess import DEVNULL
from contextlib import suppress

from tider import Request, Response
from tider.utils.url import resolve_redirect_url


class WgetResponse:

    def __init__(self, fp, status_code=200, headers=None, reason=None, url=None):
        fp.flush()
        fp.seek(0)
        self._fp = fp
        self.headers = headers or {}
        self.status_code = status_code
        self.reason = reason
        self.url = url

    def read(self, chunk_size=None):
        return self._fp.read(chunk_size)

    def close(self):
        if self._fp is not None:
            with suppress(Exception):
                self._fp.close()
            self._fp = None


class WgetDownloader:
    lazy = True

    def __init__(self, output_dir=None, quiet=True):
        self.wget_cmd = 'wget'
        self.output_dir = output_dir or '/'
        self.quiet = quiet

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        return cls(
            output_dir=settings.get('DOWNLOADER_WGET_DIR'),
            quiet=settings.getbool('DOWNLOADER_WGET_QUIET')
        )

    def download_request(self, request: Request, prepared, max_redirects=None, **_):
        ntf = None
        try:
            ntf = tempfile.NamedTemporaryFile(dir=self.output_dir)
            cmd = self.to_wget(
                request, output=ntf.name, prepared=prepared,
                max_redirects=max_redirects,
            )
            timeout = self._read_timeout(request.timeout) or 60
            timeout = max(timeout, 100)
            completed = subprocess.run(cmd, timeout=timeout, check=True, stdout=DEVNULL, stderr=subprocess.PIPE)
            status_code, headers, reason, url = self._parse_server_response(
                completed.stderr, prepared.url,
            )
            wget_response = WgetResponse(
                fp=ntf, status_code=status_code, headers=headers,
                reason=reason, url=url,
            )
            response = self._build_response(wget_response, request, prepared)
            ntf = None
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
            returncode = getattr(e, 'returncode', None)
            if returncode == 8 and ntf is not None:
                status_code, headers, reason, url = self._parse_server_response(
                    getattr(e, "stderr", None), prepared.url,
                )
                wget_response = WgetResponse(
                    fp=ntf, status_code=status_code, headers=headers,
                    reason=reason, url=url,
                )
                response = self._build_response(wget_response, request, prepared)
                ntf = None
                return response
            response = Response(request)
            response.fail(error=e)
        except Exception as e:
            response = Response(request)
            response.fail(error=e)
        finally:
            if ntf is not None:
                ntf.close()
        return response

    @staticmethod
    def _read_timeout(timeout):
        if isinstance(timeout, tuple):
            return timeout[-1]
        return timeout

    @staticmethod
    def _build_response(raw, request, prepared):
        final_prepared = prepared
        if raw.url and raw.url != prepared.url:
            final_prepared = prepared.copy_with(
                url=raw.url,
                apply_auth=False,
            )
        response = Response.from_origin_resp(
            resp=raw,
            request=request,
            prepared=final_prepared,
        )
        return response

    @staticmethod
    def _parse_server_response(stderr, url):
        status_code = 200
        reason = None
        headers = {}
        response_url = url
        redirect_location = None

        text = stderr.decode("latin-1", errors="replace") if isinstance(stderr, bytes) else str(stderr or "")
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            if line.startswith("HTTP/"):
                if redirect_location:
                    response_url = resolve_redirect_url(
                        response_url, redirect_location,
                    )
                parts = line.split(None, 2)
                try:
                    status_code = int(parts[1])
                except (IndexError, ValueError):
                    status_code = 200
                reason = parts[2] if len(parts) > 2 else None
                headers = {}
                redirect_location = None
                continue
            if ":" in line:
                key, value = line.split(":", 1)
                key, value = key.strip(), value.strip()
                headers[key] = value
                if key.lower() == "location":
                    redirect_location = value

        return status_code, headers, reason, response_url

    @staticmethod
    def _body_data(body):
        if body is None:
            return ""
        if isinstance(body, bytes):
            return body.decode()
        return str(body)

    def to_wget(self, request, output, prepared, max_redirects=None):
        wget_cmd = [self.wget_cmd]
        if self.quiet:
            wget_cmd.append('-q')
        wget_cmd.append('--tries=1')
        if not request.verify:
            wget_cmd.append('--no-check-certificate')

        timeout = self._read_timeout(request.timeout)
        if timeout:
            wget_cmd.append(f'--timeout={timeout}')
        wget_cmd.append('--server-response')

        if not request.allow_redirects:
            wget_cmd.append('--max-redirect=0')
        elif max_redirects is not None:
            wget_cmd.append(f'--max-redirect={max_redirects}')

        method = prepared.method.upper()
        if method == "POST":
            wget_cmd.append(f'--post-data={self._body_data(prepared.body)}')
        elif method != "GET":
            wget_cmd.append(f'--method={method}')
            if prepared.body is not None:
                wget_cmd.append(f'--body-data={self._body_data(prepared.body)}')

        for key, value in prepared.headers.items():
            if key.lower() == 'content-length':
                continue
            if key.lower() == 'user-agent':
                wget_cmd.extend(['-U', value])
            else:
                wget_cmd.append(f'--header={key}: {value}')

        selected_proxy = request.selected_proxy
        if selected_proxy:
            wget_cmd.extend(['-e', f'http_proxy={selected_proxy}'])
            wget_cmd.extend(['-e', f'https_proxy={selected_proxy}'])
        else:
            wget_cmd.append('--no-proxy')

        wget_cmd.extend(['-O', output, prepared.url])
        return wget_cmd

    def close(self):
        pass

    def close_expired_connections(self):
        pass
