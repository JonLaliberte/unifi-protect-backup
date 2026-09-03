"""Tests for the missing event checker's "what do we already have" query.

The checker decides what to re-download. If its lookup of already-backed-up IDs drops
any, those events get downloaded and uploaded again, which is the exact failure this
branch exists to stop. The query batches its bound parameters, so the batch boundary
needs covering.
"""

import asyncio
from datetime import datetime, timedelta, timezone

from dateutil.relativedelta import relativedelta
from uiprotect.data.nvr import Event
from uiprotect.data.types import EventType

from unifi_protect_backup.missing_event_checker import SQL_MAX_VARIABLES, MissingEventChecker
from unifi_protect_backup.utils import VideoQueue, insert_event

CAMERA = "67cb31f301131a03e401cf0e"
BASE = datetime(2026, 9, 1, 12, 0, 0, tzinfo=timezone.utc)


def make_event(n: int) -> Event:
    """Build a completed motion event."""
    return Event.model_construct(
        id=f"event-{n:04d}",
        type=EventType.MOTION,
        camera_id=CAMERA,
        start=BASE + timedelta(seconds=n),
        end=BASE + timedelta(seconds=n + 10),
        smart_detect_types=[],
    )


class _Protect:
    """Stub Protect API that serves one chunk then reports exhaustion.

    The checker keeps fetching until a chunk comes back shorter than `chunk_size`, so a
    stub that returns the same full list every time loops forever.
    """

    def __init__(self, events):
        """Init."""
        self._events = events
        self.calls = 0
        self.connect_event = asyncio.Event()
        self.connect_event.set()

    async def get_events(self, **kwargs):
        """Return the chunk once, then report exhaustion."""
        self.calls += 1
        return self._events if self.calls == 1 else []


class _Downloader:
    def __init__(self):
        """Init."""
        self.download_queue = asyncio.Queue()
        self.upload_queue = VideoQueue(1024)
        self.current_event = None


def make_checker(db, events):
    """Build a checker wired to stub Protect and downloader objects."""
    return MissingEventChecker(
        protect=_Protect(events),
        db=db,
        download_queue=asyncio.Queue(),
        downloader=_Downloader(),
        uploaders=[],
        retention=relativedelta(days=7),
        detection_types={"motion"},
        ignore_cameras=set(),
        cameras=set(),
    )


async def collect(checker):
    """Drain the checker's generator into a list of event IDs."""
    return [event.id async for event in checker._get_missing_events()]


async def test_events_already_backed_up_are_excluded(db):
    """Events already recorded must not be offered for re-download."""
    events = [make_event(n) for n in range(5)]
    await insert_event(db, events[1])
    await insert_event(db, events[3])
    await db.commit()

    assert await collect(make_checker(db, events)) == ["event-0000", "event-0002", "event-0004"]


async def test_nothing_backed_up_yields_everything(db):
    """With an empty database every event is missing."""
    events = [make_event(n) for n in range(5)]
    assert len(await collect(make_checker(db, events))) == 5


async def test_lookup_batches_without_dropping_ids(db):
    """More events than fit in one statement's bound parameters.

    A dropped ID here means a duplicate download and a duplicate upload that overwrites
    the object already in the remote, so the batch boundary is worth pinning.
    """
    count = SQL_MAX_VARIABLES * 2 + 37
    events = [make_event(n) for n in range(count)]
    for event in events:
        await insert_event(db, event)
    await db.commit()

    assert await collect(make_checker(db, events)) == []


async def test_batching_finds_the_one_missing_event_past_the_boundary(db):
    """The gap must still be found when it sits in a later batch."""
    count = SQL_MAX_VARIABLES * 2
    events = [make_event(n) for n in range(count)]
    missing = events[SQL_MAX_VARIABLES + 5]
    for event in events:
        if event is not missing:
            await insert_event(db, event)
    await db.commit()

    assert await collect(make_checker(db, events)) == [missing.id]
