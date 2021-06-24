import fileinput
import json
from typing import Any, List, Optional, Tuple, Union
import pandas as pd
from adtk.data import validate_series
from adtk.detector import GeneralizedESDTestAD
import logging
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm


LOG = logging.getLogger(__name__)

# Event represents an logon event for a user at a
# particular timestamp.
Event = Tuple[pd.Timestamp, str]


# Anomaly represents an hour, user pair for which a
# anomalous number of login events occurred.
Anomaly = Tuple[pd.Timestamp, str]


# Parse a JSON encoded line into an Event
def event(input: Union[str, bytes]) -> Event:
    e = json.loads(input)
    timestamp = pd.to_datetime(e['_source']['@timestamp'])
    user = e['_source']['user']['target']['name']
    return (timestamp, user)


class Window:

    def __init__(self, size: pd.Timedelta) -> None:
        self.events: List[Event] = []
        self.size: pd.Timedelta = size
        self.latest: Optional[pd.Timestamp] = None
        self.earliest: Optional[pd.Timestamp] = None

    def add(self, event: Event) -> None:
        self.events.append(event)
        timestamp, _ = event
        if self.earliest == None or timestamp < self.earliest:
            self.earliest = timestamp
        if self.latest == None or timestamp > self.latest:
            self.latest = timestamp

    def prune(self) -> int:
        if self.latest == None:
            return 0
        length = len(self.events)
        self.events = list(filter(lambda e: e[0] >= self.latest - self.size, self.events))
        return length - len(self.events)
    
    def saturated(self) -> bool:
        if self.earliest is None or self.latest is None:
            return False
        return self.latest - self.earliest >= self.size
  

    def check(self, event: Event) -> List[Anomaly]:
        timestamp, user = event

        # Init dataframe based on window
        df = pd.DataFrame(self.events, columns=['timestamp', 'user'])

        # Restrict dataframe just to the relevant user
        df = df[df['user'] == user]

        # Model won't work if it's empty, so return
        if len(df) < 1:
            return []

        # Bucket events by hour
        df['timestamp'] = df['timestamp'].dt.round('H')

        # Train model
        training = validate_series(df.groupby('timestamp').timestamp.count())
        esd_ad = GeneralizedESDTestAD()
        anomalies = esd_ad.fit_detect(training)

        # Reduce to only positive findings
        anomalies = anomalies[anomalies == True]

        # Convert to list of anomalies
        result = list(map(lambda a: (a[0], user), anomalies.items()))

        return result

    
        




def main(input: fileinput.FileInput, window_size: pd.Timedelta) -> None:

    # Init sliding window of events to use as model.
    window = Window(window_size)
    count: int = 0

    # For each input event we update the window, and then test the event against
    # the model.
    for line in tqdm(input, desc='events', maxinterval=1):
        count = count + 1
        e = event(line)
        window.add(e)

        # Skip checking the event until the window is saturated.
        if not window.saturated():
            continue

        window.prune()
        results = window.check(e)

        for anomaly in results:
            hour, user = anomaly
            LOG.info(f'anomaly: {user} {hour}')





if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    with logging_redirect_tqdm():
        main(fileinput.input(), pd.to_timedelta('1d'))
        LOG.info('done')
