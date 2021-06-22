import json
from pathlib import Path
import pandas as pd
from adtk.data import validate_series
from adtk.visualization import plot
from adtk.detector import GeneralizedESDTestAD
import matplotlib.pyplot as plt

'''
Functions to recreate the anomaly detection + frequency tables of the 'Anomaly Detection Exploration' HTML file. 

The HTML file was originally generated in R Markdown using R packages for anomaly detection and a human element of 
exploratory data analysis. The HTML file is an initial exploration for the feasibility of anomaly detection and is 
not currently in a productionisable state.
'''


def clean_up_keys(dictionary):
    new_dictionary = {}
    for key, value in dictionary.items():
        if key.startswith('_'):
            new_dictionary[key[1:]] = value
            continue
        new_dictionary[key] = value
    return new_dictionary


def parse_to_csv(file_object):
    '''
    :param file_object:
    :return:

    Function to parse provided jsons into same-format CSVs, originally for ease of use in R Markdown
    '''
    with Path(f'data/flat_{file_object.stem}.csv').open('wb') as csv_writer:
        row = []
        csv_writer.write(
            'timestamp,process,event_code,event_module,event_action,key,count,user,tenant,agent_name,agent_os,'
            'computer,logon_type,new_process,parent_process,index,type,id,score,hour\n'.encode()
        )
        for line in file_object.open('r', encoding='utf-8-sig'):
            log_entry = clean_up_keys(json.loads(line))
            source = log_entry.get('source', '')
            if source != '':
                row.append(source["@timestamp"])
                process = source.get('process', '')
                if process != '':
                    row.append(process.get('name', ''))
                event = source.get('event', '')
                if event != '':
                    row.append(event.get('code'))
                    row.append(event.get('module'))
                    row.append(event.get('action'))
                row.append(source.get('key'))
                row.append(source.get('count'))
                row.append(source['user']['target'].get('name', ''))
                row.append(source.get('tenant', ''))
                agent = source.get('agent', '')
                if agent != '':
                    row.append(agent.get('name', ''))
                    row.append(agent.get('os_full', ''))
                data = source.get('data', '')
                if data != '':
                    row.append(data['win']['system']['computer'])
                    event_data = data['win'].get('eventdata', '')
                    if event_data != '':
                        row.append(event_data.get('logonType'))
                        row.append(event_data.get('newProcessName', ''))
                        row.append(event_data.get('parentProcessName', ''))
            row.append(log_entry.get('index', ''))
            row.append(log_entry.get('type', ''))
            row.append(log_entry.get('id', ''))
            row.append(log_entry.get('score', ''))
            row.append(log_entry.get('hour', ''))
            csv_row = str(row)[1:-1].replace("'", "") + '\n'
            try:
                csv_writer.write(csv_row.replace(', ', ',').encode())
            except UnicodeEncodeError:
                pass
            row = []


def detect_individual_timeseries_anomalies(csv_file, type="user", name='SYSTEM'):
    '''
    :param csv_file: location of csv files containing user or process data
    :param type: string of user, new_process, or parent_process
    :param name: specific name of interest
    :return: displays a time series & flagged anomalies

    Function will display a specific time series and detect anomalies that deviate from the normal distribution of
    values.

    The R script originally used the R 'anomalize' library that detected time series anomalies by fitting daily/weekly
    trends and flagging outliers, but the equivalent function in the Python adtk library (SeasonalAD) struggled with
    the limited data. It's been replaced here with the GeneralizedESDTestAD for similar functionality.

    There are multiple OOTB anomaly detection functions in adtk documented here:
    https://adtk.readthedocs.io/en/stable/notebooks/demo.html

    Or alternative time series Python packages (such as fbprophet) are available
    '''

    df = pd.read_csv(csv_file)
    df[type] = df[type].str.replace("\\", "")
    df = df[df[type] == name]
    df['time'] = pd.to_datetime(df.timestamp).dt.round("H")
    s = validate_series(df.groupby('time').timestamp.count())
    esd_ad = GeneralizedESDTestAD()
    anomalies = esd_ad.fit_detect(s)
    print("Anomalies detected for {} {}: {}".format(type, name, sum(anomalies)))
    plot(s, anomaly=anomalies, ts_linewidth=1, ts_markersize=3, anomaly_markersize=5, anomaly_color='red',
         anomaly_tag="marker")
    plt.show()


def detect_all_timeseries_anomalies(csv_file, verbose=False, type="new_process"):
    '''
    :param csv_file: location of csv files containing user or process data
    :param verbose: boolean, print line by line?
    :param type: string of user, new_process, or parent_process
    :return: dataframe of type and count of anomalies detected

    Function to count number of anomalies for all time series in data

    Used in the R markdown to flag users/processes of interest, e.g:
    - users/processes with only 1 anomaly (a spike, such as user admin147 or user DWM-3)
    - users/processes with multiple anomalies (non-human log in patterns that don't neatly show outliers, such as
                                               sherry.hua@rhipe.com)

    The R script originally used the R 'anomalize' library that detected time series anomalies by fitting daily/weekly
    trends and flagging outliers, but the equivalent function in the Python adtk library (SeasonalAD) struggled with
    the limited data. It's been replaced here with the GeneralizedESDTestAD for similar functionality.

    There are multiple OOTB anomaly detection functions in adtk documented here:
    https://adtk.readthedocs.io/en/stable/notebooks/demo.html

    Or alternative time series Python packages (such as fbprophet) are available
    '''
    df = pd.read_csv(csv_file)
    df_out = []
    df[type] = df[type].str.replace("\\", "")
    for individual in df[type].unique():
        tmp = df[df[type] == individual].copy()
        tmp['time'] = pd.to_datetime(tmp.timestamp).dt.round("H")
        s = validate_series(tmp.groupby('time').timestamp.count())
        esd_ad = GeneralizedESDTestAD()
        anomalies = esd_ad.fit_detect(s)
        if verbose:
            print("Anomalies detected for {} {}: {}".format(type, individual, sum(anomalies)))
        df_out.append((individual,sum(anomalies)))
    cols = [type, 'anomalies_detected']
    result = pd.DataFrame(df_out, columns=cols)
    return result


def rare_pairs(csv_file):
    '''
    :param csv_file: input file, assumes it's a process dataset
    :return: aggregated dataframe

    Function aggregates parent+child processes and sorts them by rarity, and returns for human examination of outliers
    '''

    df = pd.read_csv(csv_file)
    df['new_process'] = df['new_process'].str.replace("\\", "")
    df['parent_process'] = df['parent_process'].str.replace("\\", "")
    return df.groupby(['new_process','parent_process']).size().sort_values(ascending=True).reset_index(name='pair frequency')


def rare_parents(csv_file, tolerance=1):
    '''
    :param csv_file: input file, assumes it's a process dataset
    :param tolerance: % tolerance for what is defined as a rare parent process
    :return: aggregated dataframe

    Function aggregates child+parent processes, sorts them by child-frequency, subsets to rare processes, and returns
    for human examination of outliers

    Used to find children spawned from unusual parents, such as the
    'C:/Users/admin147/AppData/Local/Temp/svchost.exe > C:/Windows/System32/cmd.exe'
    example
    '''
    df = pd.read_csv(csv_file)
    df['new_process'] = df['new_process'].str.replace("\\", "")
    df['parent_process'] = df['parent_process'].str.replace("\\", "")
    gb = df\
        .merge(df.groupby(['new_process']).size().reset_index(name='child_frequency'))\
        .merge(df.groupby(['new_process','parent_process']).size().reset_index(name='pair_frequency'))\
        .assign(percentage=lambda x: 100 * x.pair_frequency / x.child_frequency) \
        .sort_values(by=['child_frequency'],ascending=False)
    return gb.loc[gb['percentage'] < tolerance]


if __name__ == '__main__':

    # parse jsons in data folder into csvs
    for file in Path('./data').glob('*.json'):
        parse_to_csv(file)

    # detect potential anomalies in user / child / parent processes
    tmp1 = detect_all_timeseries_anomalies('data/flat_4688.csv', type="new_process") #can also examine parent_process
    tmp2 = detect_all_timeseries_anomalies('data/flat_4624.csv', type="user")

    # examine specific user or process time series
    detect_individual_timeseries_anomalies('data/flat_4624.csv', type="user", name="admin147")
    detect_individual_timeseries_anomalies('data/flat_4688.csv', type="parent_process", name="C:WindowsSystem32cmd.exe")

    # frequency of rare process pairs
    tmp3 = rare_pairs('data/flat_4688.csv')

    # children with rare parents
    tmp4 = rare_parents('data/flat_4688.csv', tolerance=1)

    print("Done!")