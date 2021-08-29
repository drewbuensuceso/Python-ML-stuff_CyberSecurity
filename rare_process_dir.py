#!/usr/bin/env python3

from io import TextIOWrapper
from typing import Any, Dict, List, Optional, Tuple, Union
import argparse
import json
import pandas as pd
import structlog


Event = Tuple[pd.Timestamp, str, Any]


# Parse a JSON encoded line into an Event
def event(input: Union[str, bytes]) -> Event:
    e = json.loads(input)
    timestamp = pd.to_datetime(e['_source']['@timestamp'])
    data = e['_source']['data']['win']['eventdata']
    process_path = data['newProcessName']
    segments = process_path.split("\\")
    dir = "\\".join(segments[:-1])
    return (timestamp, dir, e)


class Model:
    def __init__(self, size: pd.Timedelta):
        self.seen: Dict[str, pd.Timestamp] = {}
        self.size: pd.Timedelta = size

    def check(self, event: Event) -> bool:

        timestamp, dir, _ = event
        
        # Have we seen this process dir within the window?
        seen = False
        if dir in self.seen:
            if self.seen[dir] >= timestamp - self.size:
                seen = True

        # Record when we saw this process dir
        self.seen[dir] = timestamp

        return not seen


def duration(value: str) -> pd.Timedelta:
    return pd.to_timedelta(value)


def main(skip: pd.Timedelta, window: pd.Timedelta, input: TextIOWrapper):

    log = structlog.get_logger(detector='rare_process_dir')

    # Track process dirs seen in window
    model = Model(window)

    n = 0
    start: Optional[pd.Timestamp] = None

    # Loop over input
    for line in input:
        n = n + 1

        # Parse event
        e = event(line)
        timestamp, dir, full_event = e

        if start == None:
            start = timestamp + skip

        # Check against model
        anomaly = model.check(e)
        
        # Alert if necessary
        if timestamp >= start:
            if anomaly:
                ts = timestamp.to_pydatetime().isoformat()
                log.info('rare process dir detected', launch_time=ts, dir=dir, full_event=full_event)






if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Flag rare process directories in event stream')
    parser.add_argument('--skip', type=duration, metavar='WINDOW', default='30 days', help='Skip detection for events in initial WINDOW')
    parser.add_argument('--window', type=duration, default='30 days', help='Remember directories seen within the given window')
    parser.add_argument('--input', type=open, help='File containing event stream')
    args = parser.parse_args()

    main(args.skip, args.window, args.input)


