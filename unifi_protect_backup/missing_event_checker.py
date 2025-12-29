# noqa: D100

import asyncio
import logging
from datetime import datetime, timezone
from typing import AsyncIterator, Dict, List, Set

from sqlite3 import IntegrityError

import aiosqlite
from dateutil.relativedelta import relativedelta
from uiprotect import ProtectApiClient
from uiprotect.data.nvr import Event
from uiprotect.data.types import EventType

from unifi_protect_backup import VideoDownloader, VideoUploader
from unifi_protect_backup.utils import EVENT_TYPES_MAP, wanted_event_type

logger = logging.getLogger(__name__)


class MissingEventChecker:
    """Periodically checks if any unifi protect events exist within the retention period that are not backed up."""

    def __init__(
        self,
        protect: ProtectApiClient,
        db: aiosqlite.Connection,
        download_queue: asyncio.Queue,
        downloader: VideoDownloader,
        uploaders: List[VideoUploader],
        retention: relativedelta,
        detection_types: Set[str],
        ignore_cameras: Set[str],
        cameras: Set[str],
        camera_retentions: Dict[str, relativedelta] | None = None,
        interval: int = 60 * 5,
    ) -> None:
        """Init.

        Args:
            protect (ProtectApiClient): UniFi Protect API client to use
            db (aiosqlite.Connection): Async SQLite database to check for missing events
            download_queue (asyncio.Queue): Download queue to check for on-going downloads
            downloader (VideoDownloader): Downloader to check for on-going downloads
            uploaders (List[VideoUploader]): Uploaders to check for on-going uploads
            retention (relativedelta): Default retention period to limit search window
            detection_types (Set[str]): Detection types wanted to limit search
            ignore_cameras (Set[str]): Ignored camera IDs to limit search
            cameras (Set[str]): Included (ONLY) camera IDs to limit search
            camera_retentions (Dict[str, relativedelta]): Optional dictionary mapping camera IDs to retention periods.
                                                          Used to filter out events that were intentionally purged.
            interval (int): How frequently, in seconds, to check for missing events,

        """
        self._protect: ProtectApiClient = protect
        self._db: aiosqlite.Connection = db
        self._download_queue: asyncio.Queue = download_queue
        self._downloader: VideoDownloader = downloader
        self._uploaders: List[VideoUploader] = uploaders
        self.retention: relativedelta = retention
        self.camera_retentions: Dict[str, relativedelta] = camera_retentions if camera_retentions is not None else {}
        self.detection_types: Set[str] = detection_types
        self.ignore_cameras: Set[str] = ignore_cameras
        self.cameras: Set[str] = cameras
        self.interval: int = interval

    async def _get_missing_events(self) -> AsyncIterator[Event]:
        start_time = datetime.now() - self.retention
        end_time = datetime.now()
        chunk_size = 500

        while True:
            # Get list of events that need to be backed up from unifi protect
            logger.extra_debug(f"Fetching events for interval: {start_time} - {end_time}")  # type: ignore
            events_chunk = await self._protect.get_events(
                start=start_time,
                end=end_time,
                types=list(EVENT_TYPES_MAP.keys()),
                limit=chunk_size,
            )

            if not events_chunk:
                break  # There were no events to backup

            # Filter out on-going events
            unifi_events = {event.id: event for event in events_chunk if event.end is not None}

            if not unifi_events:
                break  # No completed events to process

            # Next chunks start time should be the start of the oldest complete event in the current chunk
            start_time = max([event.start for event in unifi_events.values() if event.end is not None])

            # Get list of events that have been backed up from the database

            # events(id, type, camera_id, start, end)
            async with self._db.execute("SELECT * FROM events") as cursor:
                rows = await cursor.fetchall()
                db_event_ids = {row[0] for row in rows}

            # Prevent re-adding events currently in the download/upload queue
            downloading_event_ids = {event.id for event in self._downloader.download_queue._queue}  # type: ignore
            current_download = self._downloader.current_event
            if current_download is not None:
                downloading_event_ids.add(current_download.id)

            uploading_event_ids = {event.id for event, video in self._downloader.upload_queue._queue}  # type: ignore
            for uploader in self._uploaders:
                current_upload = uploader.current_event
                if current_upload is not None:
                    uploading_event_ids.add(current_upload.id)

            existing_ids = db_event_ids | downloading_event_ids | uploading_event_ids
            missing_events = {
                event_id: event for event_id, event in unifi_events.items() if event_id not in existing_ids
            }

            # Exclude events of unwanted types
            wanted_events = {
                event_id: event
                for event_id, event in missing_events.items()
                if wanted_event_type(event, self.detection_types, self.cameras, self.ignore_cameras)
            }

            # Filter out events that are older than their camera's retention period
            # This prevents re-downloading events that were intentionally purged
            # Events from API are timezone-aware (UTC), so use UTC for comparison
            now = datetime.now(timezone.utc)
            within_retention_events = {}
            for event_id, event in wanted_events.items():
                # Determine retention period for this camera
                camera_retention = self.camera_retentions.get(event.camera_id, self.retention)

                # Skip events that are older than their camera's retention period
                # These were intentionally purged, not missing
                # Ensure event.end is timezone-aware for comparison
                if event.end is not None:
                    # If event.end is naive, assume UTC (though it shouldn't be from API)
                    event_end = event.end if event.end.tzinfo is not None else event.end.replace(tzinfo=timezone.utc)
                    if event_end < (now - camera_retention):
                        logger.extra_debug(  # type: ignore
                            f"Skipping event {event_id} from camera {event.camera_id}: "
                            f"older than retention period ({camera_retention})"
                        )
                        continue

                within_retention_events[event_id] = event

            # Yeild events one by one to allow the async loop to start other task while
            # waiting on the full list of events
            for event in within_retention_events.values():
                yield event

            # Last chunk was in-complete, we can stop now
            if len(events_chunk) < chunk_size:
                break

    async def ignore_missing(self):
        """Ignore missing events by adding them to the event table."""
        logger.info(" Ignoring missing events")

        async for event in self._get_missing_events():
            logger.extra_debug(f"Ignoring event '{event.id}'")
            try:
                await self._db.execute(
                    "INSERT INTO events VALUES "
                    f"('{event.id}', '{event.type.value}', '{event.camera_id}',"
                    f"'{event.start.timestamp()}', '{event.end.timestamp()}')"
                )
            except IntegrityError:
                logger.debug(f"Event {event.id} already exists in database, skipping")
        await self._db.commit()

    async def start(self):
        """Run main loop."""
        logger.info("Starting Missing Event Checker")
        while True:
            try:
                shown_warning = False

                # Wait for unifi protect to be connected
                await self._protect.connect_event.wait()

                logger.debug("Running check for missing events...")

                async for event in self._get_missing_events():
                    if not shown_warning:
                        logger.warning(" Found missing events, adding to backup queue")
                        shown_warning = True

                    if event.type != EventType.SMART_DETECT:
                        event_name = f"{event.id} ({event.type.value})"
                    else:
                        event_name = f"{event.id} ({', '.join(event.smart_detect_types)})"

                    logger.extra_debug(
                        f" Adding missing event to backup queue: {event_name}"
                        f" ({event.start.strftime('%Y-%m-%dT%H-%M-%S')} -"
                        f" {event.end.strftime('%Y-%m-%dT%H-%M-%S')})"
                    )
                    await self._download_queue.put(event)

            except Exception as e:
                logger.error(
                    "Unexpected exception occurred during missing event check:",
                    exc_info=e,
                )

            await asyncio.sleep(self.interval)
