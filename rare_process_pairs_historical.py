#!/usr/bin/env python3

from io import TextIOWrapper
import json
from typing import Any, List, NewType, Tuple, Union
import argparse
import pandas as pd


Process = NewType('Process', str)
Parent = NewType('Parent', str)


Event = Tuple[pd.Timestamp, Process, Parent]


def event(input: Union[str, bytes]) -> Event:
    e = json.loads(input)
    data = e['_source']['data']['win']['eventdata']
    process_name = data['newProcessName']
    parent_name = data['parentProcessName']
    timestamp = pd.to_datetime(e['_source']['@timestamp'])
    return (timestamp, process_name, parent_name)
    

def main(input: TextIOWrapper) -> str:

    events: List[Event] = []

    for line in input:
        events.append(event(line))
    
    df = pd.DataFrame(data=events, columns=['timestamp', 'child', 'parent'])

    parent_freq = df['parent'].value_counts(sort=True).reset_index().rename({'index': 'parent', 'parent': 'parent_freq'}, axis='columns')
    child_freq = df['child'].value_counts(sort=True).reset_index().rename({'index': 'child', 'child': 'child_freq'}, axis='columns')
    pair_freq = df[['parent', 'child']].value_counts(sort=True).reset_index().rename({0: 'pair_freq'}, axis='columns')

    freq = pd.merge(pair_freq, child_freq, on='child', how='left')
    freq = pd.merge(freq, parent_freq, on='parent', how='left')
    freq = freq.sort_values(['pair_freq', 'child_freq', 'parent_freq'])

    return freq.to_json(orient='records', lines=True)
   



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Rank process pairs by frequency')
    parser.add_argument('--input', type=open, help='File containing process event data')
    args = parser.parse_args()

    output = main(args.input)
    print(output)