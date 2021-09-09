#!/usr/bin/env python3

from io import TextIOWrapper
import json
from typing import Any, Dict, List, NewType, Tuple, Union
import argparse
import pandas as pd
import numpy as np
from pandas.core.reshape.merge import merge


Process = NewType('Process', str)
Parent = NewType('Parent', str)
UserTargetName = NewType('Parent', str)
SystemComputer = NewType('Parent', str)
Tenant = NewType('Parent', str)


Event = Tuple[pd.Timestamp, Process, Parent, UserTargetName, SystemComputer, Tenant]


def lists_to_dict(df):
    df.loc[:, 'user.name'] = df[['user.name']] + df[['username_freq']]
    return(df)

def event(input: Union[str, bytes]) -> Event:
    e = json.loads(input)
    data = e['_source']['data']['win']['eventdata']
    process_name = data['newProcessName']
    parent_name = data['parentProcessName']
    timestamp = pd.to_datetime(e['_source']['@timestamp'])
    user_target_name = e['_source']['user']['target']['name']
    system_computer = e['_source']['data']['win']['system']['computer']
    tenant = e['_source']['tenant']
    return (timestamp, process_name, parent_name, user_target_name, system_computer, tenant)

def main(input: TextIOWrapper) -> Dict[str, Any]:

    events: List[Event] = []

    count = 0
    skipped = 0
    for line in input:
        count = count + 1
        try:
            events.append(event(line))
        except:
            skipped = skipped + 1

    df = pd.DataFrame(data=events, columns=['timestamp', 'child', 'parent', 'user.name', 'system.computer', 'tenant'])
    grouped = df.groupby(['parent', 'child'])
    #user.name processing
    username_distinct_freq = df.groupby(['parent', 'child'])['user.name'].nunique().reset_index(name="uniq_usernames")[['parent', 'child', 'uniq_usernames']]
    username_freq = grouped['user.name'].value_counts().reset_index(name="username_freq")
    username_freq.loc[:, 'user.name'] = username_freq['user.name'] + ":" + username_freq['username_freq'].astype(str)
    username_freq.drop(columns=['username_freq'])
    username_list = username_freq.groupby(['parent', 'child'])['user.name'].agg(lambda x: list(x)).reset_index()
    
    usernames = pd.merge(username_distinct_freq,username_list, on=['parent', 'child'])

    #system.computer processing
    system_distinct_freq = df.groupby(['parent', 'child'])['system.computer'].nunique().reset_index(name="uniq_systems")[['parent', 'child', 'uniq_systems']]
    system_freq = grouped['system.computer'].value_counts().reset_index(name="system_freq")
    system_freq.loc[:, 'system.computer'] = system_freq['system.computer'] + ":" + system_freq['system_freq'].astype(str)
    system_freq.drop(columns=['system_freq'])
    system_list = system_freq.groupby(['parent', 'child'])['system.computer'].agg(lambda x: list(x)).reset_index()
    
    systems = pd.merge(system_distinct_freq, system_list, on=['parent', 'child'])

    #tenant processing
    tenant_distinct_freq = df.groupby(['parent', 'child'])['tenant'].nunique().reset_index(name="uniq_tenants")[['parent', 'child', 'uniq_tenants']]
    tenant_freq = grouped['tenant'].value_counts().reset_index(name="tenant_freq")
    tenant_freq.loc[:, 'tenant'] = tenant_freq['tenant'] + ":" + tenant_freq['tenant_freq'].astype(str)
    tenant_freq.drop(columns=['tenant_freq'])
    tenant_list = tenant_freq.groupby(['parent', 'child'])['tenant'].agg(lambda x: list(x)).reset_index()
    
    tenants = pd.merge(tenant_distinct_freq, tenant_list, on=['parent', 'child'])
    
    parent_freq = df['parent'].value_counts(sort=True).reset_index().rename({'index': 'parent', 'parent': 'parent_freq'}, axis='columns')
    child_freq = df['child'].value_counts(sort=True).reset_index().rename({'index': 'child', 'child': 'child_freq'}, axis='columns')
    pair_freq = df[['parent', 'child']].value_counts(sort=True).reset_index().rename({0: 'pair_freq'}, axis='columns')


    freq = pd.merge(pair_freq, child_freq, on='child', how='left')
    freq = pd.merge(freq, parent_freq, on='parent', how='left')
    freq = pd.merge(freq, usernames, on=['parent', 'child'])
    freq = pd.merge(freq, systems, on=['parent', 'child'])
    freq = pd.merge(freq, tenants, on=['parent', 'child'])
    freq = freq.sort_values(['pair_freq', 'child_freq', 'parent_freq'])
    
    print(freq)
    result = freq.to_dict(orient='records')
    return {'meta': {'events': count, 'skipped': skipped}, 'process_pair_rarity': result}




if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Rank process pairs by frequency')
    parser.add_argument('--input', type=open, help='File containing process event data')
    args = parser.parse_args()

    output = main(args.input)
    print(json.dumps(output))