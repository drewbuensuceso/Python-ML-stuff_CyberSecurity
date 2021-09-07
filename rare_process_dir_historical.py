#!/usr/bin/env python3

from io import TextIOWrapper
from typing import Any, Dict, List, Optional, Tuple, Union
import argparse
import json
import pandas as pd


Event = Tuple[pd.Timestamp, str]


# Parse a JSON encoded line into an Event
def event(input: Union[str, bytes]) -> Event:
    e = json.loads(input)
    timestamp = pd.to_datetime(e['_source']['@timestamp'])
    data = e['_source']['data']['win']['eventdata']
    process_path = data['newProcessName']
    segments = process_path.split("\\")
    dir = "\\".join(segments[:-1])
    return (timestamp, dir)



def main(input: TextIOWrapper) -> Dict[str, Any]:
    events: List[Event] = []
    total = 0
    skipped = 0
    for line in input:
        total = total + 1
        try:
            events.append(event(line))
        except:
            skipped = skipped + 1
    df = pd.DataFrame(data=events, columns=['timestamp', 'dir'])
    freq = df['dir'].value_counts(sort=True, ascending=True).reset_index().rename({'index': 'dir', 'dir': 'count'}, axis='columns')
    dirs = freq.to_dict(orient='records')
    return {'meta': {'events': total, 'skipped': skipped}, 'process_dir_rarity': dirs}




if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='List process directories by rarity')
    parser.add_argument('--input', type=open, help='File containing event data')
    args = parser.parse_args()

    print(json.dumps(main(args.input)))


