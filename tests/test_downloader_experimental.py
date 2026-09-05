"""Tests for the local-only unique export filename patch.

Each retry has to prepare a name no other client has used, so a name another client
already claimed cannot poison the attempts that follow.
"""

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from uiprotect.api import ProtectApiClient
from uiprotect.data import Version
from uiprotect.exceptions import BadRequest

from unifi_protect_backup.downloader_experimental import VideoDownloaderExperimental
from unifi_protect_backup.uiprotect_patch import monkey_patch_experimental_downloader
from unifi_protect_backup.utils import VideoQueue


# The prepare/download methods under test are installed onto the client by this patch,
# so it has to run before they can be referenced.
monkey_patch_experimental_downloader()

START = datetime(2026, 9, 1, 12, 0, 0, tzinfo=timezone.utc)


def FakeEvent():
    """Build the parts of an event that `_download` reads."""
    return SimpleNamespace(
        id="f9f5a34b-867d-4001-9b42-c3429c1785df",
        camera_id="cam1",
        start=START,
        end=START + timedelta(seconds=30),
    )


class FakeProtect:
    """Exercise the patched client methods against a controlled NVR response."""

    NEW_DOWNLOAD_VERSION = Version("4.0.0")
    prepare_camera_video = ProtectApiClient.prepare_camera_video  # type: ignore[attr-defined]
    download_camera_video = ProtectApiClient.download_camera_video  # type: ignore[attr-defined]

    def __init__(self):
        """Record prepare/download calls and fail the first download."""
        self.bootstrap = SimpleNamespace(nvr=SimpleNamespace(version=Version("7.2.105")))
        self.prepared_names: list[str] = []
        self.download_calls: list[tuple[str, bool]] = []

    async def _validate_channel_id(self, camera_id, channel_index):
        return None

    async def api_request(self, url, *, params, raise_exception):
        """Return the prepared filename exactly as Protect does."""
        assert url == "video/prepare"
        assert raise_exception is True
        self.prepared_names.append(params["filename"])
        return {"fileName": params["filename"]}

    async def api_request_raw(self, url, *, params, raise_exception):
        """Simulate a stale-name 404 followed by a successful retry."""
        assert url == "video/download"
        self.download_calls.append((params["filename"], raise_exception))
        if len(self.download_calls) == 1:
            raise BadRequest("Request failed: video/download - Status: 404")
        return b"video"


async def test_retry_uses_fresh_export_name_and_preserves_http_error(monkeypatch):
    """A stale server-side name must not poison the next download attempt."""
    tokens = iter(("first", "second"))
    monkeypatch.setattr("unifi_protect_backup.uiprotect_patch.secrets.token_hex", lambda _: next(tokens))

    async def no_sleep(_):
        return None

    monkeypatch.setattr("unifi_protect_backup.downloader_experimental.asyncio.sleep", no_sleep)

    protect = FakeProtect()
    downloader = VideoDownloaderExperimental(
        protect=protect,
        db=None,
        download_queue=asyncio.Queue(),
        upload_queue=VideoQueue(1024),
        color_logging=False,
        download_rate_limit=None,
        max_event_length=timedelta(minutes=10),
    )

    video = await downloader._download(FakeEvent())

    assert video == b"video"
    assert protect.prepared_names == [
        "cam1 09-01-2026, 12.00.00 UTC - 09-01-2026, 12.00.30 UTC first.mp4",
        "cam1 09-01-2026, 12.00.00 UTC - 09-01-2026, 12.00.30 UTC second.mp4",
    ]
    assert protect.download_calls == [
        (protect.prepared_names[0], True),
        (protect.prepared_names[1], True),
    ]
