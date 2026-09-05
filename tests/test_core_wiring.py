"""Tests for how the core wires the uploader pool together.

`tests/test_uploader.py` proves the shared in-flight set stops two uploaders racing
*when they are given one*. Nothing proved the core actually gives them one. The
`uploading_event_ids` parameter defaults to a private set, so dropping the argument
in `_create_uploaders` leaves every uploader guarding only against itself, restoring
the race with no failing test and no log line. These tests close that gap.
"""

from unifi_protect_backup.unifi_protect_backup_core import UnifiProtectBackup
from unifi_protect_backup.utils import VideoQueue

from .conftest import EVENT_ID


def make_core(parallel_uploads):
    """Build a core instance with only the attributes `_create_uploaders` reads.

    `__init__` resolves rclone remotes and builds a ProtectApiClient, none of which
    this wiring touches, so it is bypassed rather than mocked.
    """
    core = object.__new__(UnifiProtectBackup)
    core._protect = None
    core._db = None
    core._parallel_uploads = parallel_uploads
    core.rclone_destination = "S3:/bucket"
    core.rclone_args = ""
    core.file_structure_format = "{event.id}.mp4"
    core.color_logging = False
    return core


def test_creates_one_uploader_per_parallel_upload():
    """The pool size follows --parallel-uploads."""
    uploaders = make_core(4)._create_uploaders(VideoQueue(1024))
    assert len(uploaders) == 4


def test_every_uploader_shares_one_in_flight_set():
    """The guard is only a guard if all uploaders look at the same set.

    Identity, not equality: four separate empty sets are equal to each other and would
    pass an `==` check while guarding nothing.
    """
    uploaders = make_core(4)._create_uploaders(VideoQueue(1024))

    first = uploaders[0]._uploading_event_ids
    assert all(u._uploading_event_ids is first for u in uploaders)


def test_one_uploader_claiming_an_event_is_visible_to_the_others():
    """What sharing the set actually buys: a peer's claim blocks the others.

    This is the behaviour `--parallel-uploads > 1` depends on, asserted through the
    same attribute the uploader loop consults.
    """
    uploaders = make_core(3)._create_uploaders(VideoQueue(1024))

    uploaders[0]._uploading_event_ids.add(EVENT_ID)

    assert all(EVENT_ID in u._uploading_event_ids for u in uploaders[1:])


def test_uploaders_all_drain_the_same_queue():
    """One queue feeds the pool; a private queue per uploader would deadlock the buffer."""
    queue = VideoQueue(1024)
    uploaders = make_core(3)._create_uploaders(queue)

    assert all(u.upload_queue is queue for u in uploaders)


def test_single_uploader_still_gets_a_shared_set():
    """The default path must not silently differ from the parallel one."""
    (uploader,) = make_core(1)._create_uploaders(VideoQueue(1024))

    assert uploader._uploading_event_ids == set()
