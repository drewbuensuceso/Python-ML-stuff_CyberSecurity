#!/usr/bin/env python3

from io import TextIOWrapper
import json
from typing import Any, List, NewType, Optional, Tuple, Union
import structlog
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
    

def main(training_input: TextIOWrapper, input: TextIOWrapper) -> None:

    log = structlog.get_logger(detector='rare_process_pairs')

    # Make set of all (process, parent) pairs seen in training data
    seen = set()

    # Load training data into model.
    for line in training_input:
        seen.add(event(line))
    
    log.info('training data loaded', known_pairs=len(seen))
   
    # Check for unseen (process, parent) pairs in main input
    for line in input:
      
        e = event(line)
        if not seen.__contains__(e):
            timestamp, process, parent = e
            ts = timestamp.to_pydatetime().isoformat()
            log.info('rare process pair detected', time=ts, process=process, parent=parent)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Flag unknown process pairs in event stream')
    parser.add_argument('--input', type=open, help='File containing event stream')
    parser.add_argument('--training', type=open, help='File containing training data')
    args = parser.parse_args()

    main(args.training, args.input)