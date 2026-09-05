# noqa: D100

import logging
import pathlib
import re
from datetime import datetime
from typing import Optional, Set

import aiosqlite
from uiprotect import ProtectApiClient
from uiprotect.data.nvr import Event

from unifi_protect_backup.utils import (
    SubprocessException,
    VideoQueue,
    get_camera_name,
    human_readable_size,
    insert_event,
    run_command,
    setup_event_logger,
)


class VideoUploader:
    """Uploads videos from the video_queue to the provided rclone destination.

    Keeps a log of what its uploaded in `db`
    """

    def __init__(
        self,
        protect: ProtectApiClient,
        upload_queue: VideoQueue,
        rclone_destination: str,
        rclone_args: str,
        file_structure_format: str,
        db: aiosqlite.Connection,
        color_logging: bool,
        uploading_event_ids: Optional[Set[str]] = None,
    ):
        """Init.

        Args:
            protect (ProtectApiClient): UniFi Protect API client to use
            upload_queue (VideoQueue): Queue to get video files from
            rclone_destination (str): rclone file destination URI
            rclone_args (str): arguments to pass to the rclone command
            file_structure_format (str): format string for how to structure the uploaded files
            db (aiosqlite.Connection): Async SQlite database connection
            color_logging (bool):  Whether or not to add color to logging output
            uploading_event_ids (Optional[Set[str]]): IDs currently being uploaded, shared by every
                uploader. The database check alone cannot stop two uploaders racing, since
                the row is only written once the upload finishes. Defaults to a private set,
                which is correct when there is only one uploader.

        """
        self._protect: ProtectApiClient = protect
        self.upload_queue: VideoQueue = upload_queue
        self._rclone_destination: str = rclone_destination
        self._rclone_args: str = rclone_args
        self._file_structure_format: str = file_structure_format
        self._db: aiosqlite.Connection = db
        self.current_event = None
        self._uploading_event_ids: Set[str] = uploading_event_ids if uploading_event_ids is not None else set()

        self.base_logger = logging.getLogger(__name__)
        setup_event_logger(self.base_logger, color_logging)
        self.logger = logging.LoggerAdapter(self.base_logger, {"event": ""})

    async def start(self):
        """Run main loop.

        Runs forever looking for video data in the video queue and then uploads it
        using rclone, finally it updates the database
        """
        self.logger.info("Starting Uploader")
        while True:
            try:
                event, video = await self.upload_queue.get()
                # Set before the checks below so the missing event checker's in-flight
                # guard can still see this event while we decide what to do with it.
                self.current_event = event

                self.logger = logging.LoggerAdapter(self.base_logger, {"event": f" [{event.id}]"})

                self.logger.info(f"Uploading event: {event.id}")
                self.logger.debug(
                    f" Remaining Upload Queue: {self.upload_queue.qsize_files()}"
                    f" ({human_readable_size(self.upload_queue.qsize())})"
                )

                # Claim the event before the first await. Checking and adding with no
                # suspension point in between is what makes this atomic across uploaders;
                # the database check below cannot do it, because the row is not written
                # until the upload finishes.
                if event.id in self._uploading_event_ids:
                    self.logger.debug(f" Event {event.id} is already being uploaded, skipping")
                    self.current_event = None
                    continue
                self._uploading_event_ids.add(event.id)

                try:
                    # Check before writing, not after. `rclone rcat` overwrites silently,
                    # so uploading an event that is already backed up destroys the object
                    # in the remote and, on storage with a minimum retention like Glacier,
                    # bills for the early delete. Detecting the duplicate afterwards costs
                    # the full download and upload regardless of whether it is caught.
                    if await self._is_recorded(event):
                        self.logger.debug(f" Event {event.id} already backed up, skipping upload")
                        continue

                    destination = await self._generate_file_path(event)
                    self.logger.debug(f" Destination: {destination}")

                    try:
                        await self._upload_video(video, destination, self._rclone_args)
                        if await self._update_database(event, destination):
                            self.logger.debug("Uploaded")
                        else:
                            # Lost a race the checks above could not see. The upload has
                            # already happened and overwritten the existing object.
                            self.logger.warning(f" Event {event.id} was already backed up; this upload overwrote it")
                    except SubprocessException:
                        self.logger.error(f" Failed to upload file: '{destination}'")
                finally:
                    self._uploading_event_ids.discard(event.id)
                    self.current_event = None

            except Exception as e:
                self.logger.error(f"Unexpected exception occurred, abandoning event {event.id}:", exc_info=e)

    async def _is_recorded(self, event: Event) -> bool:
        """Return True if a completed backup for this event is already in the database.

        Only sees *finished* backups: the row is written after rclone returns. The
        in-flight set in `start()` covers the window this cannot.

        The lookup is served entirely by the primary key's index
        (`SEARCH events USING COVERING INDEX`), so it does not read the table.
        """
        async with self._db.execute("SELECT 1 FROM events WHERE id = ?", (event.id,)) as cursor:
            return await cursor.fetchone() is not None

    async def _upload_video(self, video: bytes, destination: pathlib.Path, rclone_args: str):
        """Upload video using rclone.

        In order to avoid writing to disk, the video file data is piped directly
        to the rclone process and uploaded using the `rcat` function of rclone.

        Args:
            video (bytes): The data to be written to the file
            destination (pathlib.Path): Where rclone should write the file
            rclone_args (str): Optional extra arguments to pass to `rclone`

        Raises:
            RuntimeError: If rclone returns a non-zero exit code

        """
        returncode, stdout, stderr = await run_command(f'rclone rcat -vv {rclone_args} "{destination}"', video)
        if returncode != 0:
            raise SubprocessException(stdout, stderr, returncode)

    async def _update_database(self, event: Event, destination: pathlib.Path) -> bool:
        """Add the backed up event to the database along with where it was backed up to.

        Returns:
            bool: False if this event was already recorded, in which case no `backups` row
                  is written either. A duplicate event must not gain a second backup row.

        """
        if not await insert_event(self._db, event):
            # Nothing of ours was written, but the INSERT still opened a transaction and
            # took the write lock. Commit to close it rather than leaving it for whichever
            # unrelated task commits next. Rollback would be wrong here: every component
            # shares this connection, so it would discard their pending writes too.
            await self._db.commit()
            return False

        # Split once: the remote name is everything before the first colon, and an
        # rclone path may legally contain further colons.
        remote, file_path = str(destination).split(":", 1)
        await self._db.execute(
            "INSERT INTO backups VALUES (?, ?, ?)",
            (event.id, remote, file_path),
        )

        await self._db.commit()
        return True

    async def _generate_file_path(self, event: Event) -> pathlib.Path:
        """Generate the rclone destination path for the provided event.

        Generates rclone destination path for the given even based upon the format string
        in `self.file_structure_format`.

        Provides the following fields to the format string:
          event: The `Event` object as per
                 https://github.com/briis/uiprotect/blob/master/uiprotect/data/nvr.py
          duration_seconds: The duration of the event in seconds
          detection_type: A nicely formatted list of the event detection type and the smart detection types (if any)
          camera_name: The name of the camera that generated this event

        Args:
            event: The event for which to create an output path

        Returns:
            pathlib.Path: The rclone path the event should be backed up to

        """
        assert isinstance(event.camera_id, str)
        assert isinstance(event.start, datetime)
        assert isinstance(event.end, datetime)

        format_context = {
            "event": event,
            "duration_seconds": (event.end - event.start).total_seconds(),
            "detection_type": f"{event.type.value} ({' '.join(event.smart_detect_types)})"
            if event.smart_detect_types
            else f"{event.type.value}",
            "camera_name": await get_camera_name(self._protect, event.camera_id),
        }

        file_path = self._file_structure_format.format(**format_context)
        file_path = re.sub(r"[^\w\-_\.\(\)/ ]", "", file_path)  # Sanitize any invalid chars

        return pathlib.Path(f"{self._rclone_destination}/{file_path}")
