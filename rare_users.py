#!/usr/bin/env python3

from io import TextIOWrapper
from typing import List, Optional, Tuple, Union
import argparse
import json
import pandas as pd
import structlog


Event = Tuple[pd.Timestamp, str]


# Parse a JSON encoded line into an Event
def event(input: Union[str, bytes]) -> Event:
    e = json.loads(input)
    timestamp = pd.to_datetime(e['_source']['@timestamp'])
    user = e['_source']['user']['target']['name']
    return (timestamp, user)


class Model:
    def __init__(self, size: pd.Timedelta):
        self.events: List[Event] = []
        self.size: pd.Timedelta = size
        self.latest: Optional[pd.Timestamp] = None

    def check(self, event: Event) -> bool:

        timestamp, user = event
       
        # Update time
        if self.latest == None or timestamp > self.latest:
            self.latest = timestamp
        
        # Prune expired events
        self.events = list(filter(lambda e: e[0] >= self.latest - self.size, self.events))
        
        # Check if this event has been seen within window
        seen = any(map(lambda e: e[1] == user, self.events))

        # Record that we have seen this event
        self.events.append(event)

        return not seen


def duration(value: str) -> pd.Timedelta:
    return pd.to_timedelta(value)


def main(skip: pd.Timedelta, window: pd.Timedelta, input: TextIOWrapper):

    log = structlog.get_logger(detector='rare_users')

    # Track known users (seen within window)
    model = Model(window)

    n = 0
    start: Optional[pd.Timestamp] = None

    # Loop over input
    for line in input:
        n = n + 1

        # Parse event
        e = event(line)
        timestamp, user = e

        if start == None:
            start = timestamp + skip

        # Check against model
        anomaly = model.check(e)

        #log.debug('read event', logon_time=timestamp, user=user)
        
        # Alert if necessary
        if timestamp >= start:
            if anomaly:
                ts = timestamp.to_pydatetime().isoformat()
                log.info('rare user detected', logon_time=ts, user=user)






if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Flag rare users in event stream')
    parser.add_argument('--skip', type=duration, metavar='WINDOW', default='30 days', help='Skip detection for events in initial WINDOW')
    parser.add_argument('--window', type=duration, default='30 days', help='Remember users in the given window')
    parser.add_argument('--input', type=open, help='File containing event stream')
    args = parser.parse_args()

    main(args.skip, args.window, args.input)


