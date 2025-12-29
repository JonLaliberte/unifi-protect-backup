# noqa: D100

import logging
import time
from datetime import datetime
from typing import Dict

import aiosqlite
from dateutil.relativedelta import relativedelta

from unifi_protect_backup.utils import format_retention, run_command, wait_until

logger = logging.getLogger(__name__)


async def delete_file(file_path, rclone_purge_args):
    """Delete `file_path` via rclone."""
    returncode, stdout, stderr = await run_command(f'rclone delete -vv "{file_path}" {rclone_purge_args}')
    if returncode != 0:
        logger.error(f" Failed to delete file: '{file_path}'")


async def tidy_empty_dirs(base_dir_path):
    """Delete any empty directories in `base_dir_path` via rclone."""
    returncode, stdout, stderr = await run_command(f'rclone rmdirs -vv --ignore-errors --leave-root "{base_dir_path}"')
    if returncode != 0:
        logger.error(" Failed to tidy empty dirs")


class Purge:
    """Deletes old files from rclone remotes."""

    def __init__(
        self,
        db: aiosqlite.Connection,
        default_retention: relativedelta,
        rclone_destination: str,
        interval: relativedelta | None,
        rclone_purge_args: str = "",
        camera_retentions: Dict[str, relativedelta] | None = None,
    ):
        """Init.

        Args:
            db (aiosqlite.Connection): Async SQlite database connection to purge clips from
            default_retention (relativedelta): Default retention period for cameras without specific retention
            rclone_destination (str): What rclone destination the clips are stored in
            interval (relativedelta): How often to purge old clips
            rclone_purge_args (str): Optional extra arguments to pass to `rclone delete` directly.
            camera_retentions (Dict[str, relativedelta]): Optional dictionary mapping camera IDs to retention periods.

        """
        self._db: aiosqlite.Connection = db
        self.default_retention: relativedelta = default_retention
        self.camera_retentions: Dict[str, relativedelta] = camera_retentions if camera_retentions is not None else {}
        self.rclone_destination: str = rclone_destination
        self.interval: relativedelta = interval if interval is not None else relativedelta(days=1)
        self.rclone_purge_args: str = rclone_purge_args

    async def start(self):
        """Run main loop."""
        while True:
            try:
                deleted_a_file = False

                # Get all unique camera IDs from the events table
                async with self._db.execute("SELECT DISTINCT camera_id FROM events") as camera_cursor:
                    camera_rows = await camera_cursor.fetchall()
                    camera_ids = [row[0] for row in camera_rows]

                # Process each camera separately with its own retention period
                for camera_id in camera_ids:
                    # Determine retention period for this camera
                    retention = self.camera_retentions.get(camera_id, self.default_retention)

                    # Calculate retention cutoff time for this camera
                    retention_oldest_time = time.mktime((datetime.now() - retention).timetuple())

                    # Query events for this specific camera that are older than the cutoff
                    async with self._db.execute(
                        "SELECT * FROM events WHERE camera_id = ? AND end < ?", (camera_id, retention_oldest_time)
                    ) as event_cursor:
                        async for event_id, event_type, event_camera_id, event_start, event_end in event_cursor:  # noqa: B007
                            retention_str = format_retention(retention)
                            logger.info(f"Purging event: {event_id} (camera: {event_camera_id}, retention: {retention_str})")

                            # For every backup for this event
                            async with self._db.execute(
                                "SELECT * FROM backups WHERE id = ?", (event_id,)
                            ) as backup_cursor:
                                async for _, remote, file_path in backup_cursor:
                                    await delete_file(f"{remote}:{file_path}", self.rclone_purge_args)
                                    logger.debug(f" Deleted: {remote}:{file_path}")
                                    deleted_a_file = True

                            # delete event from database
                            # entries in the `backups` table are automatically deleted by sqlite triggers
                            await self._db.execute("DELETE FROM events WHERE id = ?", (event_id,))
                            await self._db.commit()

                if deleted_a_file:
                    await tidy_empty_dirs(self.rclone_destination)

            except Exception as e:
                logger.error("Unexpected exception occurred during purge:", exc_info=e)

            next_purge_time = datetime.now() + self.interval
            logger.extra_debug(f"sleeping until {next_purge_time}")
            await wait_until(next_purge_time)
