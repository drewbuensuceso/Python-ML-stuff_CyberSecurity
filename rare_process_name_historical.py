#!/usr/bin/env python3

from io import TextIOWrapper
from typing import Any, Dict, List, Optional, Tuple, Union
import argparse
import json
import pandas as pd
import structlog


Event = Tuple[pd.Timestamp, str]


# Parse a JSON encoded line into an Event
def event(input: Union[str, bytes]) -> Event:
    e = json.loads(input)
    timestamp = pd.to_datetime(e['_source']['@timestamp'])
    data = e['_source']['data']['win']['eventdata']
    process_path = data['newProcessName']
    segments = process_path.split("\\")
    name = segments[-1]
    return (timestamp, name)


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
    df = pd.DataFrame(data=events, columns=['timestamp', 'name'])
    freq = df['name'].value_counts(sort=True, ascending=True).reset_index().rename({'index': 'name', 'name': 'count'}, axis='columns')
    names = freq.to_dict(orient='records')
    return {'meta': {'events': total, 'skipped': skipped}, 'process_name_rarity': names}






if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='List rare process names by rarity')
    parser.add_argument('--input', type=open, help='File containing event data')
    args = parser.parse_args()

    print(json.dumps(main(args.input)))


