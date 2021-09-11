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
    user_target_name = e['_source']['user']['target']['name']
    system_computer = e['_source']['data']['win']['system']['computer']
    tenant = e['_source']['tenant']
    return (timestamp, name, user_target_name, system_computer, tenant)

def get_whitelisted(input: TextIOWrapper, nwl: TextIOWrapper) -> Dict[str, Any]:
    events: List[Event] = []

    total = 0
    skipped = 0
    for name_wl in nwl:
        for line in input:
            try:
                if event(line)[1] == name_wl:
                    events.append(event(line))
                    total += 1
                else:
                    skipped = skipped + 1
            except:
                skipped = skipped + 1
    return(total, skipped, events)

def get_all_names(input: TextIOWrapper) -> Dict[str, Any]:
    events: List[Event] = []
    total = 0
    skipped = 0
    for line in input:
        total = total + 1
        try:
            events.append(event(line))
        except:
            skipped = skipped + 1
    return(total, skipped, events)

def main(input: TextIOWrapper, nwl: TextIOWrapper) -> Dict[str, Any]:
    
    if (input is not None) & (nwl is not None):
        total, skipped, events = get_whitelisted(input, nwl)
    else:
        total, skipped, events = get_all_names(input)

    df = pd.DataFrame(data=events, columns=['timestamp', 'name', 'user.name', 'system.computer', 'tenant'])
    grouped = df.groupby(['name'])
    '''user.name processing'''
    username_distinct_freq = df.groupby(['name'])['user.name'].nunique().reset_index(name="uniq_usernames")[['name', 'uniq_usernames']]
    username_freq = grouped['user.name'].value_counts().reset_index(name="username_freq")
    username_freq.loc[:, 'user.name'] = username_freq['user.name'] + ":" + username_freq['username_freq'].astype(str)
    username_freq.drop(columns=['username_freq'])
    username_list = username_freq.groupby(['name'])['user.name'].agg(lambda x: list(x)).reset_index()
    
    usernames = pd.merge(username_list, username_distinct_freq, on=['name'])

    '''system.computer processing'''
    system_distinct_freq = df.groupby(['name'])['system.computer'].nunique().reset_index(name="uniq_systems")[['name', 'uniq_systems']]
    system_freq = grouped['system.computer'].value_counts().reset_index(name="system_freq")
    system_freq.loc[:, 'system.computer'] = system_freq['system.computer'] + ":" + system_freq['system_freq'].astype(str)
    system_freq.drop(columns=['system_freq'])
    system_list = system_freq.groupby(['name'])['system.computer'].agg(lambda x: list(x)).reset_index()
    
    systems = pd.merge(system_list, system_distinct_freq, on=['name'])

    '''tenant processing'''
    tenant_distinct_freq = df.groupby(['name'])['tenant'].nunique().reset_index(name="uniq_tenants")[['name', 'uniq_tenants']]
    tenant_freq = grouped['tenant'].value_counts().reset_index(name="tenant_freq")
    tenant_freq.loc[:, 'tenant'] = tenant_freq['tenant'] + ":" + tenant_freq['tenant_freq'].astype(str)
    tenant_freq.drop(columns=['tenant_freq'])
    tenant_list = tenant_freq.groupby(['name'])['tenant'].agg(lambda x: list(x)).reset_index()
    
    tenants = pd.merge(tenant_list, tenant_distinct_freq, on=['name'])

    freq = df['name'].value_counts(sort=True, ascending=True).reset_index().rename({'index': 'name', 'name': 'count'}, axis='columns')
    freq = pd.merge(freq, usernames, on=['name'], how='left')
    freq = pd.merge(freq, systems, on=['name'], how='left')
    freq = pd.merge(freq, tenants, on=['name'], how='left')
    # freq = pd.merge(freq, df[['name', 'user.name', 'system.computer', 'tenant']], on=['name'], how='left')
    
    names = freq.to_dict(orient='records')
    return {'meta': {'events': total, 'skipped': skipped}, 'process_name_rarity': names}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='List rare process names by rarity')
    parser.add_argument('--input', type=open, help='File containing event data')
    parser.add_argument('--nwl', type=open, help='File containing text file for whitelisting names')
    args = parser.parse_args()

    output = main(args.input, args.nwl)

    print(json.dumps(output))


