"""Tests for the uploader's duplicate guards.

The uploader used to detect duplicates by catching the IntegrityError raised when writing
the database row, which happens *after* rclone has already written to the remote. Since
`rclone rcat` overwrites silently, the duplicate always landed and destroyed the object
already there. These tests pin the check ahead of the upload.
"""

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

import pytest

from unifi_protect_backup.unifi_protect_backup_core import create_database
from unifi_protect_backup.uploader import VideoUploader
from unifi_protect_backup.utils import SubprocessException, VideoQueue, insert_event

START = datetime(2026, 9, 1, 12, 0, 0, tzinfo=timezone.utc)
EVENT_ID = "f9f5a34b-867d-4001-9b42-c3429c1785df"


@dataclass
class _Type:
    value: str


@dataclass
class _Event:
    id: str
    type: _Type = field(default_factory=lambda: _Type("motion"))
    camera_id: str = "cam1"
    start: datetime = START
    end: datetime = START + timedelta(seconds=30)


class _Uploader(VideoUploader):
    """VideoUploader with the two external dependencies replaced.

    Only rclone and the camera-name lookup are stubbed. The queue, the database, the
    guards and the ordering between them are all the real thing, which is the point.
    """

    def __init__(self, db, uploading_event_ids=None, fail_upload=False):
        super().__init__(
            protect=None,
            upload_queue=VideoQueue(512 * 1024 * 1024),
            rclone_destination="S3:/bucket",
            rclone_args="",
            file_structure_format="{event.id}.mp4",
            db=db,
            color_logging=False,
            uploading_event_ids=uploading_event_ids,
        )
        self.uploaded: list[str] = []
        self._fail_upload = fail_upload

    async def _generate_file_path(self, event):
        return f"S3:/bucket/{event.id}.mp4"

    async def _upload_video(self, video, destination, rclone_args):
        if self._fail_upload:
            raise SubprocessException("", "boom", 1)
        self.uploaded.append(destination)


@pytest.fixture
async def db():
    """Build an in-memory database using the real production schema."""
    connection = await create_database(":memory:")
    yield connection
    await connection.close()


async def drain(*uploaders):
    """Run the uploader loops until their queues empty, then stop them."""
    tasks = [asyncio.create_task(u.start()) for u in uploaders]
    for _ in range(200):
        await asyncio.sleep(0.005)
        if all(u.upload_queue.qsize_files() == 0 for u in uploaders):
            break
    await asyncio.sleep(0.02)
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


async def test_new_event_is_uploaded_and_recorded(db):
    """The happy path still works."""
    uploader = _Uploader(db)
    await uploader.upload_queue.put((_Event(EVENT_ID), b"video"))
    await drain(uploader)

    assert uploader.uploaded == [f"S3:/bucket/{EVENT_ID}.mp4"]
    async with db.execute("SELECT COUNT(*) FROM events") as cursor:
        assert (await cursor.fetchone())[0] == 1
    async with db.execute("SELECT COUNT(*) FROM backups") as cursor:
        assert (await cursor.fetchone())[0] == 1


async def test_already_backed_up_event_is_not_uploaded(db):
    """The fix. An event already in the database must not reach rclone.

    Before this, the duplicate was detected by the failing insert *after* rclone had
    overwritten the object in the remote.
    """
    await insert_event(db, _Event(EVENT_ID))
    await db.commit()

    uploader = _Uploader(db)
    await uploader.upload_queue.put((_Event(EVENT_ID), b"video"))
    await drain(uploader)

    assert uploader.uploaded == []


async def test_same_event_queued_twice_uploads_once(db):
    """Two copies reaching the uploader cost one upload, not two."""
    uploader = _Uploader(db)
    await uploader.upload_queue.put((_Event(EVENT_ID), b"video"))
    await uploader.upload_queue.put((_Event(EVENT_ID), b"video"))
    await drain(uploader)

    assert len(uploader.uploaded) == 1


async def test_parallel_uploaders_do_not_both_upload(db):
    """With --parallel-uploads > 1 the database check alone is not enough.

    Both uploaders can read the table before either has written to it, since the row is
    only inserted once rclone returns. The shared in-flight set is what closes that.
    """
    shared: set[str] = set()
    a = _Uploader(db, uploading_event_ids=shared)
    b = _Uploader(db, uploading_event_ids=shared)
    # One queue feeding both uploaders, as production wires it.
    b.upload_queue = a.upload_queue
    await a.upload_queue.put((_Event(EVENT_ID), b"video"))
    await a.upload_queue.put((_Event(EVENT_ID), b"video"))
    await drain(a, b)

    assert len(a.uploaded) + len(b.uploaded) == 1
    assert shared == set()  # released again


async def test_failed_upload_leaves_no_row(db):
    """A failed upload must stay retryable by the missing event checker."""
    uploader = _Uploader(db, fail_upload=True)
    await uploader.upload_queue.put((_Event(EVENT_ID), b"video"))
    await drain(uploader)

    async with db.execute("SELECT COUNT(*) FROM events") as cursor:
        assert (await cursor.fetchone())[0] == 0


async def test_in_flight_id_is_released_after_failure(db):
    """A crash mid-upload must not permanently block that event."""
    shared: set[str] = set()
    uploader = _Uploader(db, uploading_event_ids=shared, fail_upload=True)
    await uploader.upload_queue.put((_Event(EVENT_ID), b"video"))
    await drain(uploader)

    assert shared == set()


async def test_duplicate_does_not_write_a_second_backups_row(db):
    """If a duplicate somehow reaches the insert, it must not gain a backups row."""
    uploader = _Uploader(db)
    await uploader.upload_queue.put((_Event(EVENT_ID), b"video"))
    await drain(uploader)

    # Simulate losing the race: the guards are bypassed, the upload happened anyway.
    assert await uploader._update_database(_Event(EVENT_ID), "S3:/bucket/x.mp4") is False
    async with db.execute("SELECT COUNT(*) FROM backups") as cursor:
        assert (await cursor.fetchone())[0] == 1
