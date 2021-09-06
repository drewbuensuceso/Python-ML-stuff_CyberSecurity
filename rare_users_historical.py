#!/usr/bin/env python3

from io import TextIOWrapper
from typing import Any, Dict, List, Optional, Tuple, Union
import argparse
import json
import pandas as pd
import os


Event = Tuple[pd.Timestamp, str]


# Parse a JSON encoded line into an Event
def event(input: Union[str, bytes]) -> Event:
    e = json.loads(input)
    timestamp = pd.to_datetime(e['_source']['@timestamp'])
    user = e['_source']['user']['target']['name']
    return (timestamp, user)







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
    df = pd.DataFrame(data=events, columns=['timestamp', 'user'])
    freq = df['user'].value_counts(sort=True, ascending=True).reset_index().rename({'index': 'user', 'user': 'count'}, axis='columns')
    users = freq.to_dict(orient='records')
    return {'meta': {'events': total, 'skipped': skipped}, 'user_rarity': users}





if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='List users in dataset by rarity')
    parser.add_argument('--input', type=open, help='File containing dataset')
    args = parser.parse_args()

    print(json.dumps(main(args.input)))


