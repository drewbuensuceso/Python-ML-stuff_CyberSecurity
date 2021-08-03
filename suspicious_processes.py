import fileinput
import sys
import json
from typing import Any, List, NewType, Optional, Tuple, Union
import logging
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm


LOG = logging.getLogger(__name__)

Process = NewType('Process', str)
Parent = NewType('Parent', str)


Event = Tuple[Process, Parent]


def event(input: Union[str, bytes]) -> Event:
    e = json.loads(input)
    data = e['_source']['data']['win']['eventdata']
    process_name = data['newProcessName']
    parent_name = data['parentProcessName']
    return (process_name, parent_name)

    
        

def main(training_input: fileinput.FileInput, input: fileinput.FileInput) -> None:

    # Make set of all (process, parent) pairs seen in training data
    seen = set()

    # Load training data into model.
    for line in training_input:
        seen.add(event(line))
   
    # Check for unseen (process, parent) pairs in main input
    for line in tqdm(input, desc='events', maxinterval=1):
      
        e = event(line)
        if not seen.__contains__(e):
            process, parent = e
            LOG.info(f'anomaly: {parent} -> {process}')



if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    with logging_redirect_tqdm():
        f = sys.argv[1]
        main(fileinput.input(f), fileinput.input('-'))
        LOG.info('done')
