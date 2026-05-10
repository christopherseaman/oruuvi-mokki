#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "ruuvitag-sensor==4.1.0",
# ]
# ///
"""Listen for Ruuvi BLE advertisements and persist each reading to SQLite."""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

from ruuvitag_sensor.ruuvi import RuuviTagSensor

PROJECT_DIR = Path(__file__).resolve().parent
DEFAULT_DB = PROJECT_DIR / "data" / "ruuvi.db"
SCHEMA_FILE = PROJECT_DIR / "schema.sql"

INSERT_SQL = """
INSERT INTO readings (
    received_at, mac, data_format,
    temperature_c, humidity_pct, pressure_hpa,
    acceleration_x_mg, acceleration_y_mg, acceleration_z_mg,
    battery_mv, tx_power_dbm,
    movement_counter, measurement_sequence, rssi_dbm
) VALUES (
    :received_at, :mac, :data_format,
    :temperature_c, :humidity_pct, :pressure_hpa,
    :acceleration_x_mg, :acceleration_y_mg, :acceleration_z_mg,
    :battery_mv, :tx_power_dbm,
    :movement_counter, :measurement_sequence, :rssi_dbm
)
"""

FIELD_MAP = {
    "data_format": "data_format",
    "temperature": "temperature_c",
    "humidity": "humidity_pct",
    "pressure": "pressure_hpa",
    "acceleration_x": "acceleration_x_mg",
    "acceleration_y": "acceleration_y_mg",
    "acceleration_z": "acceleration_z_mg",
    "battery": "battery_mv",
    "tx_power": "tx_power_dbm",
    "movement_counter": "movement_counter",
    "measurement_sequence_number": "measurement_sequence",
    "rssi": "rssi_dbm",
}


def open_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, isolation_level=None)  # autocommit
    conn.executescript(SCHEMA_FILE.read_text())
    return conn


def to_row(mac: str, payload: dict) -> dict:
    row = {dst: payload.get(src) for src, dst in FIELD_MAP.items()}
    row["mac"] = mac
    row["received_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    return row


async def run(db_path: Path, macs: list[str] | None) -> None:
    log = logging.getLogger("oruuvi")
    conn = open_db(db_path)
    log.info("writing to %s; filter=%s", db_path, macs or "all")

    n = 0
    try:
        async for mac, payload in RuuviTagSensor.get_data_async(macs or []):
            conn.execute(INSERT_SQL, to_row(mac, payload))
            n += 1
            if n % 30 == 0:
                log.info("%d readings written; latest %s temp=%s°C batt=%smV",
                         n, mac, payload.get("temperature"), payload.get("battery"))
    finally:
        conn.close()
        log.info("stopped after %d readings", n)


def parse_macs(raw: str) -> list[str]:
    return [m.strip().upper() for m in raw.split(",") if m.strip()]


def main() -> int:
    logging.basicConfig(
        level=os.environ.get("ORUUVI_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stderr,
    )
    db_path = Path(os.environ.get("ORUUVI_DB", DEFAULT_DB)).expanduser()
    macs = parse_macs(os.environ.get("ORUUVI_MACS", ""))

    loop = asyncio.new_event_loop()
    stop = asyncio.Event()

    def _stop(*_):
        loop.call_soon_threadsafe(stop.set)

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _stop)

    task = loop.create_task(run(db_path, macs))

    async def _supervise():
        done, _ = await asyncio.wait(
            {task, asyncio.create_task(stop.wait())},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    try:
        loop.run_until_complete(_supervise())
    finally:
        loop.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
