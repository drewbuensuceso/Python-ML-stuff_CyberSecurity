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
    dir = "\\".join(segments[:-1]).replace('\\\\', '\\')
    user_target_name = e['_source']['user']['target']['name']
    system_computer = e['_source']['data']['win']['system']['computer']
    tenant = e['_source']['tenant']
    return (timestamp, dir, user_target_name, system_computer, tenant)

def get_whitelisted(input: TextIOWrapper, dwl: TextIOWrapper) -> Dict[str, Any]:
    events: List[Event] = []

    total = 0
    skipped = 0
    for dir_wl in dwl:
        print(dir_wl)
        for line in input:
            try:
                if event(line)[1] == dir_wl:
                    events.append(event(line))
                    total += 1
                else:
                    skipped = skipped + 1
            except:
                skipped = skipped + 1
    return(total, skipped, events)

def get_all_dirs(input: TextIOWrapper) -> Dict[str, Any]:
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

def main(input: TextIOWrapper, dwl: TextIOWrapper) -> Dict[str, Any]:
    
    if (input is not None) & (dwl is not None):
        total, skipped, events = get_whitelisted(input, dwl)
    else:
        total, skipped, events = get_all_dirs(input)

    df = pd.DataFrame(data=events, columns=['timestamp', 'dir', 'user.name', 'system.computer', 'tenant'])
    grouped = df.groupby(['dir'])
    '''user.name processing'''
    username_distinct_freq = df.groupby(['dir'])['user.name'].nunique().reset_index(name="uniq_usernames")[['dir', 'uniq_usernames']]
    username_freq = grouped['user.name'].value_counts().reset_index(name="username_freq")
    username_freq.loc[:, 'user.name'] = username_freq['user.name'] + ":" + username_freq['username_freq'].astype(str)
    username_freq.drop(columns=['username_freq'])
    username_list = username_freq.groupby(['dir'])['user.name'].agg(lambda x: list(x)).reset_index()
    
    usernames = pd.merge(username_list, username_distinct_freq, on=['dir'])

    '''system.computer processing'''
    system_distinct_freq = df.groupby(['dir'])['system.computer'].nunique().reset_index(name="uniq_systems")[['dir', 'uniq_systems']]
    system_freq = grouped['system.computer'].value_counts().reset_index(name="system_freq")
    system_freq.loc[:, 'system.computer'] = system_freq['system.computer'] + ":" + system_freq['system_freq'].astype(str)
    system_freq.drop(columns=['system_freq'])
    system_list = system_freq.groupby(['dir'])['system.computer'].agg(lambda x: list(x)).reset_index()
    
    systems = pd.merge(system_list, system_distinct_freq, on=['dir'])

    '''tenant processing'''
    tenant_distinct_freq = df.groupby(['dir'])['tenant'].nunique().reset_index(name="uniq_tenants")[['dir', 'uniq_tenants']]
    tenant_freq = grouped['tenant'].value_counts().reset_index(name="tenant_freq")
    tenant_freq.loc[:, 'tenant'] = tenant_freq['tenant'] + ":" + tenant_freq['tenant_freq'].astype(str)
    tenant_freq.drop(columns=['tenant_freq'])
    tenant_list = tenant_freq.groupby(['dir'])['tenant'].agg(lambda x: list(x)).reset_index()
    
    tenants = pd.merge(tenant_list, tenant_distinct_freq, on=['dir'])
    
    freq = df['dir'].value_counts(sort=True, ascending=True).reset_index().rename({'index': 'dir', 'dir': 'count'}, axis='columns')
    freq = pd.merge(freq, usernames, on=['dir'], how='left')
    freq = pd.merge(freq, systems, on=['dir'], how='left')
    freq = pd.merge(freq, tenants, on=['dir'], how='left')
    dirs = freq.to_dict(orient='records')
    return {'meta': {'events': total, 'skipped': skipped}, 'process_dir_rarity': dirs}




if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='List process directories by rarity')
    parser.add_argument('--input', type=open, help='File containing event data')
    parser.add_argument('--dwl', type=open, help='File containing directory whitelist data')
    args = parser.parse_args()
    
    output =main(args.input, args.dwl)
    print(json.dumps(output))


