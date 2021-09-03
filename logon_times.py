#!/usr/bin/env python3

import json
from typing import Any, Dict, List, Optional, Tuple, Union
import pandas as pd
from adtk.data import validate_series
from adtk.detector import GeneralizedESDTestAD
from io import TextIOWrapper
import structlog
import argparse


# Event represents an logon event for a user at a
# particular timestamp.
Event = Tuple[pd.Timestamp, str]


# Anomaly represents an hour, user pair for which a
# anomalous number of login events occurred.
Anomaly = Tuple[pd.Timestamp, str, int]


# Parse a JSON encoded line into an Event
def event(input: Union[str, bytes]) -> Tuple[Event, Any]:
    e = json.loads(input)
    timestamp = pd.to_datetime(e['_source']['@timestamp'])
    user = e['_source']['user']['target']['name']
    return ((timestamp, user), e)


class Window:

    def __init__(self, size: pd.Timedelta) -> None:
        self.size: pd.Timedelta = size
        self.latest: Optional[pd.Timestamp] = None
        self.earliest: Optional[pd.Timestamp] = None
        self.data: Dict[str, pd.Series] = dict()

    def add(self, event: Event) -> None:
        timestamp, user = event
        if self.earliest == None or timestamp < self.earliest:
            self.earliest = timestamp.floor('H')
        if self.latest == None or timestamp > self.latest:
            self.latest = timestamp.floor('H')
        
        # Increment the count for the current user for the current hour
        hour = timestamp.floor('H')
        new_series = pd.Series(data={hour: 1})
        if user not in self.data:
            self.data[user] = new_series
        else:
            self.data[user] = self.data[user].add(new_series, fill_value=0)
        
        # Prune old buckets
        cutoff: pd.Timestamp = self.latest - self.size
        cutoff = cutoff.floor('H')
        self.data[user] = self.data[user][cutoff < self.data[user].index]

    def prune(self) -> int:
        return 0

    def saturated(self) -> bool:
        if self.earliest is None or self.latest is None:
            return False
        return self.latest - self.earliest > self.size
    
    def check(self, event: Event) -> List[Anomaly]:
        timestamp, user = event

        # Get series for user
        series = self.data.get(user)
        if series is None:
            return []

        # Setup model
        training = validate_series(series)
        esd_ad = GeneralizedESDTestAD()
        try:
            esd_ad.fit(training)
        except RuntimeError:
            return []

        # Check if the hour/bucket of our current event is anomalous
        hour = timestamp.floor('H')
        check = training[training.index == hour]
        anomalies = esd_ad.detect(check)

        # Reduce to only positive findings
        anomalies = check[anomalies == True]

        # Convert to list of anomalies
        result = list(map(lambda a: (a[0], user, a[1]), anomalies.items()))

        return result


def main(input: TextIOWrapper, window_size: pd.Timedelta) -> None:

    log = structlog.get_logger(detector='logon_times')

    # Init sliding window of events to use as model.
    window = Window(window_size)
    count: int = 0
    logged_saturation = False

    # For each input event we update the window, and then test the event against
    # the model.
    for line in input:
        count = count + 1
        e, raw = event(line)
        window.add(e)

        # Skip checking the event until the window is saturated.
        if not window.saturated():
            continue

        if not logged_saturation:
            logged_saturation = True
            log.info('model reached saturation', events=count)

        window.prune()
        results = window.check(e)

        for anomaly in results:
            hour, user, logons = anomaly
            ts = hour.to_pydatetime().isoformat()
            log.info('anomalous logon for user', user=user, hour=ts, logons=logons, raw_event=raw)


def duration(value: str) -> pd.Timedelta:
    return pd.to_timedelta(value)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Flag anomalous logon time for user')
    parser.add_argument('--window', type=duration, default='30 days', help='Model sample size')
    parser.add_argument('--input', type=open, help='File containing event stream')
    args = parser.parse_args()

    main(args.input, args.window)