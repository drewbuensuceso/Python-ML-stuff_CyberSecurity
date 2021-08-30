from typing import List
import logon_times as main
import pandas as pd

lines: List[str] = []
for line in open('data/users.txt', 'r'):
    lines.append(line)


def test_event():
    line = lines[0]
    timestamp, user = main.event(line)
    assert(timestamp == pd.to_datetime('2021-05-17T10:33:17.698Z'))
    assert(user) == 'SYSTEM'


def test_window():
    window = main.Window(pd.to_timedelta('24h'))
    assert(len(window.events) == 0)


def test_window_add():
    window = main.Window(pd.to_timedelta('24h'))
    for i in range(10):
        event = main.event(lines[i])
        window.add(event)
        assert(window.events[i] == event)


def test_window_prune():
    first = main.event(lines[0])
    last = main.event(lines[len(lines)-1])
    window_size = (last[0] - first[0])/2
    window = main.Window(window_size)
    window.add(first)
    window.add(last)
    assert(window.prune() == 1)
    assert(window.events[0][0] == last[0])


def test_saturated():
    first = main.event(lines[0])
    last = main.event(lines[len(lines)-1])
    window = main.Window(pd.to_timedelta('30d'))
    window.add(first)
    window.add(last)
    assert(not window.saturated())
    window = main.Window(pd.to_timedelta('1d'))
    window.add(first)
    window.add(last)
    assert(window.saturated())


def test_check_basic():
    window = main.Window(pd.to_timedelta('1h'))
    for line in lines[:1000]:
        window.add(main.event(line))
    assert(window.prune() > 0)
    check = main.event(lines[1001])
    assert(len(window.check(check)) == 0)


def test_check():
    window = main.Window(pd.to_timedelta('5d'))
    for line in lines:
        window.add(main.event(line))
    assert(window.prune() > 0)
    ts, _ = main.event(lines[len(lines)-1])
    anomalies = window.check((ts, 'victoria.zorin@rhipe.com'))
    assert(len(anomalies) > 0)

def test_main():
    main.main(lines, pd.to_timedelta('1d'))