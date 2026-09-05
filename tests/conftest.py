"""Shared test setup.

These tests exercise modules directly rather than through the CLI, so the custom log
levels ``setup_logging`` normally installs have to be registered here.
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

import pytest

from unifi_protect_backup.unifi_protect_backup_core import create_database
from unifi_protect_backup.utils import add_logging_level

for _name, _level in (("EXTRA_DEBUG", logging.DEBUG - 1), ("WEBSOCKET_DATA", logging.DEBUG - 2)):
    if not hasattr(logging, _name):
        add_logging_level(_name, _level)


START = datetime(2026, 9, 1, 12, 0, 0, tzinfo=timezone.utc)
EVENT_ID = "f9f5a34b-867d-4001-9b42-c3429c1785df"


@dataclass
class FakeType:
    """Stand-in for uiprotect's EventType enum."""

    value: str = "motion"


@dataclass
class FakeEvent:
    """Minimal stand-in for uiprotect's Event, carrying only what the DB layer reads.

    Used where the code under test only touches `id`, `type`, `camera_id`, `start` and
    `end`. Tests that exercise uiprotect behaviour (the websocket listener) build a real
    `Event` with `model_construct` instead.
    """

    id: str = EVENT_ID
    type: FakeType = field(default_factory=FakeType)
    camera_id: str = "cam1"
    start: datetime = START
    end: datetime = START + timedelta(seconds=30)


@pytest.fixture
async def db():
    """Build an in-memory database using the real production schema."""
    connection = await create_database(":memory:")
    yield connection
    await connection.close()
