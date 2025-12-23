import csv
import datetime as dt
import random
import uuid
from dataclasses import asdict, dataclass, fields
from enum import Enum

import numpy as np

NORMAL_REJECTION_PROB = 0.03
ANOMALY_REJECTION_PROB = 0.25
ANOMALY_DURATION = dt.timedelta(minutes=10)

TRADING_DAY_START = dt.datetime(2025, 12, 20, 9, 15)
TRADING_DAY_END = dt.datetime(2025, 12, 20, 15, 30)

ANOMALY_START = dt.datetime(2025, 12, 20, 12, 15)
ANOMALY_END = ANOMALY_START + ANOMALY_DURATION

FILEPATH = "data/events.csv"


class Instrument(Enum):
    Future = 1
    Option = 2
    Stock = 3


@dataclass
class Event:
    event_id: str
    event_type: str
    timestamp_created: dt.datetime
    timestamp_processed: dt.datetime
    trade_id: str
    counterparty_id: str
    instrument_id: Instrument
    exchange_id: str
    client_id: str
    pipeline_stage: str
    region: str
    status: str
    trade_volume: str
    trade_price: str


def serialize(value: object) -> str:
    if isinstance(value, dt.datetime):
        return value.isoformat()
    return str(value)


CLIENTS = {1000, 1001, 1002, 1003, 1004}


def get_latency() -> dt.timedelta:
    random_ms = random.uniform(50.0, 150.0)
    random_delta = dt.timedelta(milliseconds=random_ms)

    return random_delta


def get_time_gap() -> dt.timedelta:
    random_seconds = random.uniform(1.0, 3.0)
    random_delta = dt.timedelta(seconds=random_seconds)

    return random_delta


def get_event_status(is_anomaly: bool) -> str:
    rejection_prob = ANOMALY_REJECTION_PROB if is_anomaly else NORMAL_REJECTION_PROB
    event_status = np.random.choice(
        ["SUCCESS", "REJECTED"], p=[1 - rejection_prob, rejection_prob]
    )
    return event_status


def generate_event(time_created: dt.datetime, event_status: str) -> Event:
    event = Event(
        event_id=str(uuid.uuid4()),
        event_type="TRADE",
        timestamp_created=time_created,
        timestamp_processed=time_created + get_latency(),
        trade_id=str(uuid.uuid4()),
        counterparty_id="CP1",
        instrument_id=random.choice(list(Instrument)),
        exchange_id="ASX",
        client_id=str(random.choice(tuple(CLIENTS))),
        pipeline_stage="INITIAL",
        region="AU",
        status=event_status,
        trade_volume=str(100),
        trade_price=str(20),
    )

    return event


if __name__ == "__main__":
    current_time = TRADING_DAY_START

    with open(FILEPATH, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[f.name for f in fields(Event)])
        writer.writeheader()

        while current_time <= TRADING_DAY_END:
            if ANOMALY_START <= current_time <= ANOMALY_END:
                event_status = get_event_status(True)
            else:
                event_status = get_event_status(False)

            event = generate_event(current_time, event_status)
            current_time += get_time_gap()

            writer.writerow({k: serialize(v) for k, v in asdict(event).items()})
