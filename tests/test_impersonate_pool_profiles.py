import io
import unittest
from unittest.mock import Mock, patch

from tider import Request, Response
from tider.network.downloaders import impersonate


class FakeCurl:
    def __init__(self):
        self.closed = 0
        self.resets = 0

    def setopt(self, *args):
        pass

    def reset(self):
        self.resets += 1

    def close(self):
        self.closed += 1


class CurlPoolProfileTests(unittest.TestCase):
    def pool(self, **kwargs):
        pool = impersonate.CurlPool(curl_cls=FakeCurl, **kwargs)
        self.addCleanup(pool.close)
        return pool

    def test_same_profile_reuses_handle(self):
        pool = self.pool()
        first = pool.acquire(profile=('chrome116', False))
        curl = first.curl
        first.release()
        second = pool.acquire(profile=('chrome116', False))
        self.assertIs(second.curl, curl)
        second.release()

    def test_changed_profile_discards_cached_connections(self):
        pool = self.pool()
        first = pool.acquire(profile=('chrome116', False))
        curl = first.curl
        first.release()
        second = pool.acquire(profile=('chrome120', False))
        self.assertIsNot(second.curl, curl)
        self.assertEqual(curl.closed, 1)
        second.release()

    def test_total_idle_capacity_covers_all_profiles(self):
        pool = self.pool(maxsize=1)
        first = pool.acquire(profile='chrome116')
        second = pool.acquire(profile='chrome120')
        curl = second.curl
        first.release()
        second.release()
        self.assertEqual(len(pool._idle), 1)
        self.assertEqual(curl.closed, 1)

    def test_inflight_handles_keep_their_own_profile(self):
        pool = self.pool()
        first = pool.acquire(profile='chrome116')
        second = pool.acquire(profile='chrome120')
        first_curl, second_curl = first.curl, second.curl
        first.release()
        second.release()
        again = pool.acquire(profile='chrome116')
        self.assertIs(again.curl, first_curl)
        self.assertEqual(second_curl.closed, 1)
        again.release()

    def test_expiry_cleanup_preserves_profile(self):
        pool = self.pool(idle_ttl=10)
        with patch.object(impersonate, 'preferred_clock', return_value=0):
            first = pool.acquire(profile='chrome116')
            curl = first.curl
            first.release()
        with patch.object(impersonate, 'preferred_clock', return_value=5):
            pool.close_expired_connections()
            second = pool.acquire(profile='chrome116')
            self.assertIs(second.curl, curl)
            second.release()
        with patch.object(impersonate, 'preferred_clock', return_value=16):
            pool.close_expired_connections()
        self.assertEqual(curl.closed, 1)
        self.assertFalse(pool._idle)

    def test_reset_failure_discards_handle(self):
        pool = self.pool()
        handle = pool.acquire(profile='chrome116')
        curl = handle.curl
        curl.reset = Mock(side_effect=RuntimeError('reset failed'))
        handle.release()
        self.assertEqual(curl.closed, 1)
        self.assertFalse(pool._idle)

    def test_release_after_shutdown_closes_once(self):
        pool = self.pool()
        handle = pool.acquire(profile='chrome116')
        curl = handle.curl
        pool.close()
        handle.release()
        handle.release()
        self.assertEqual(curl.closed, 1)
        self.assertFalse(pool._idle)

    def test_unprofiled_callers_keep_reuse(self):
        pool = self.pool()
        first = pool.acquire()
        curl = first.curl
        first.release()
        second = pool.acquire()
        self.assertIs(second.curl, curl)
        second.discard()
        self.assertEqual(curl.closed, 1)


class ImpersonateProfileReuseTests(unittest.TestCase):
    def setUp(self):
        patcher = patch.object(impersonate, 'Curl', FakeCurl)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.downloader = impersonate.ImpersonateDownloader(concurrency=2)
        self.addCleanup(self.downloader.close)
        self.curls = []

        def configure(curl, **kwargs):
            self.curls.append(curl)
            kwargs['content_callback'](b'{}')
            return None, None, io.BytesIO(), None, None, None

        patcher = patch.object(impersonate, 'set_curl_options', side_effect=configure)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.downloader._perform = Mock()
        self.downloader._build_response = Mock(side_effect=lambda *args, **kwargs: Response())

    def download(self, profile='chrome116', **kwargs):
        request = Request('https://example.invalid/', impersonate=profile, **kwargs)
        self.addCleanup(request.close)
        response = self.downloader.download_request(request, prepared=request.prepare())
        self.addCleanup(response.close)
        return response

    def test_normal_entry_point_isolates_profile_changes(self):
        for profile in ('chrome116', 'chrome116', 'chrome120', 'chrome120'):
            self.download(profile).check_error()
        self.assertIs(self.curls[0], self.curls[1])
        self.assertIsNot(self.curls[1], self.curls[2])
        self.assertIs(self.curls[2], self.curls[3])
        self.assertEqual(self.curls[0].closed, 1)

    def test_http_version_change_discards_handle(self):
        self.download(http2=False).check_error()
        self.download(http2=True).check_error()
        self.assertIsNot(self.curls[0], self.curls[1])
        self.assertEqual(self.curls[0].closed, 1)

    def test_custom_profiles_bypass_shared_pool(self):
        self.download().check_error()
        reusable = self.curls[-1]
        for key in ('impersonate_ja3', 'impersonate_akamai', 'impersonate_extra_fp', 'impersonate_curl_options'):
            with self.subTest(key=key):
                self.download(meta={key: 'custom'}).check_error()
                custom = self.curls[-1]
                self.assertIsNot(custom, reusable)
                self.assertEqual(custom.closed, 1)
                self.download().check_error()
                self.assertIs(self.curls[-1], reusable)

    def test_proxy_requests_keep_independent_handles(self):
        for _ in range(2):
            self.download(proxies={'all': 'socks5h://proxy.invalid:1080'}).check_error()
        self.assertIsNot(self.curls[0], self.curls[1])
        self.assertEqual([curl.closed for curl in self.curls], [1, 1])
        self.assertFalse(self.downloader.curl_pool._idle)

    def test_failed_transfer_discards_handle(self):
        self.downloader._perform.side_effect = RuntimeError('transfer failed')
        response = self.download()
        self.assertTrue(response.failed)
        self.assertEqual(self.curls[-1].closed, 1)
        self.assertFalse(self.downloader.curl_pool._idle)
        self.downloader._perform.side_effect = None
        self.download().check_error()
        self.assertIsNot(self.curls[0], self.curls[1])

    def test_custom_failure_preserves_normal_idle_handle(self):
        self.download().check_error()
        reusable = self.curls[-1]
        self.downloader._perform.side_effect = RuntimeError('transfer failed')
        self.assertTrue(self.download(meta={'impersonate_extra_fp': {'tls_grease': True}}).failed)
        self.assertEqual(self.curls[-1].closed, 1)
        self.downloader._perform.side_effect = None
        self.download().check_error()
        self.assertIs(self.curls[-1], reusable)


if __name__ == '__main__':
    unittest.main()
