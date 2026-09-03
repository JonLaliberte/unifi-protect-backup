"""Tests for the shared `insert_event` helper.

`insert_event` is the single writer for the `events` table. Its contract matters because
callers use the return value to decide whether to write a related `backups` row — a
duplicate event must never gain a second backup row.
"""

from dataclasses import dataclass
from datetime import datetime, timezone

import pytest

from unifi_protect_backup.unifi_protect_backup_core import create_database
from unifi_protect_backup.utils import insert_event


@dataclass
class _Type:
    """Stand-in for uiprotect's EventType enum."""

    value: str


@dataclass
class _Event:
    """Minimal stand-in for uiprotect's Event, carrying only what insert_event reads."""

    id: str
    type: _Type
    camera_id: str
    start: datetime
    end: datetime


def _event(event_id: str = "f9f5a34b-867d-4001-9b42-c3429c1785df", camera_id: str = "cam1") -> _Event:
    return _Event(
        id=event_id,
        type=_Type("motion"),
        camera_id=camera_id,
        start=datetime(2026, 9, 1, 12, 0, 0, tzinfo=timezone.utc),
        end=datetime(2026, 9, 1, 12, 0, 30, tzinfo=timezone.utc),
    )


@pytest.fixture
async def db():
    """An in-memory database using the real production schema."""
    connection = await create_database(":memory:")
    yield connection
    await connection.close()


async def test_new_event_is_written(db):
    """A previously unseen event is recorded and reports success."""
    assert await insert_event(db, _event()) is True

    async with db.execute("SELECT id, type, camera_id FROM events") as cursor:
        rows = await cursor.fetchall()

    assert rows == [("f9f5a34b-867d-4001-9b42-c3429c1785df", "motion", "cam1")]


async def test_duplicate_event_returns_false(db):
    """A repeated event is rejected without raising, and does not create a second row."""
    assert await insert_event(db, _event()) is True
    assert await insert_event(db, _event()) is False

    async with db.execute("SELECT COUNT(*) FROM events") as cursor:
        assert (await cursor.fetchone())[0] == 1


async def test_duplicate_does_not_gain_a_backups_row(db):
    """The return value is what stops a duplicate event getting a second backup row.

    This is the contract callers depend on. If `insert_event` swallowed the error and
    reported success, `_update_database` would write a second `backups` row pointing at a
    file that was overwritten rather than added.
    """
    event = _event()
    if await insert_event(db, event):
        await db.execute("INSERT INTO backups VALUES (?, ?, ?)", (event.id, "S3", "/a.mp4"))

    if await insert_event(db, event):  # duplicate: must not reach the backups insert
        await db.execute("INSERT INTO backups VALUES (?, ?, ?)", (event.id, "S3", "/b.mp4"))

    await db.commit()
    async with db.execute("SELECT COUNT(*) FROM backups") as cursor:
        assert (await cursor.fetchone())[0] == 1


async def test_timestamps_round_trip_as_numbers(db):
    """Values bind as REAL so `purge`'s numeric `end < ?` comparison keeps working."""
    event = _event()
    await insert_event(db, event)

    async with db.execute("SELECT start, end FROM events") as cursor:
        start, end = await cursor.fetchone()

    assert start == event.start.timestamp()
    assert end == event.end.timestamp()
    assert isinstance(start, float)


async def test_values_are_bound_not_interpolated(db):
    """A quote in a value must not break the statement.

    Real ids and camera ids can't contain quotes today, which is why the previous
    f-string interpolation was never exploitable. Binding means it stays that way if
    Protect ever changes its id format.
    """
    event = _event(event_id="it's-a-quote", camera_id="cam'1")
    assert await insert_event(db, event) is True

    async with db.execute("SELECT id, camera_id FROM events") as cursor:
        assert await cursor.fetchone() == ("it's-a-quote", "cam'1")


async def test_does_not_commit(db):
    """The helper leaves committing to the caller so related writes share a transaction."""
    await insert_event(db, _event())
    await db.rollback()

    async with db.execute("SELECT COUNT(*) FROM events") as cursor:
        assert (await cursor.fetchone())[0] == 0
