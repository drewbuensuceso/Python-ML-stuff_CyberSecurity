Various realtime anomaly detectors

```sh
pip install -r requirements.txt
./logon_times.py --help
./suspicious_processes.py --help
./rare_users.py --help
./rare_process_name.py --help
./rare_process_dir.py --help
```

---

Process event data expected in this format:

```
{"_index":"td-ml-hids-4688-2021","_type":"_doc","_id":"tSxrY3kBBGrFFIuyjq6T","_score":1,"_source":{"process":{},"event":{"module":"ml-hids","code":"4688","action":"A new process has been created."},"input":{},"key":"td_%{[user][target][name]}_2021-05-13T01:51:02.672Z","count":1,"day":"Thu","user":{"target":{}},"agent":{"name":"DEV-SURAJ","os_full":"Microsoft Windows 10 Enterprise"},"@timestamp":"2021-05-13T01:51:02.672Z","tenant":"td","data":{"win":{"eventdata":{"newProcessName":"C:\\\\Windows\\\\System32\\\\svchost.exe","parentProcessName":"C:\\\\Windows\\\\System32\\\\services.exe"},"system":{"computer":"DEV-SURAJ"}}},"hour":"01"}}
{"_index":"td-ml-hids-4688-2021","_type":"_doc","_id":"tyxrY3kBBGrFFIuyjq6T","_score":1,"_source":{"process":{},"event":{"module":"ml-hids","code":"4688","action":"A new process has been created."},"input":{},"key":"td_%{[user][target][name]}_2021-05-13T01:51:02.673Z","count":1,"day":"Thu","user":{"target":{}},"agent":{"name":"DEV-SURAJ","os_full":"Microsoft Windows 10 Enterprise"},"@timestamp":"2021-05-13T01:51:02.673Z","tenant":"td","data":{"win":{"eventdata":{"newProcessName":"C:\\\\Windows\\\\System32\\\\wbem\\\\WmiApSrv.exe","parentProcessName":"C:\\\\Windows\\\\System32\\\\services.exe"},"system":{"computer":"DEV-SURAJ"}}},"hour":"01"}}
```

User logon event data expected in this format:

```
"_index":"td-ml-hids-4624-2021","_type":"_doc","_id":"HIbjeXkBtnvU9Dr0IFrZ","_score":1,"_source":{"process":{"name":"C:\\\\Windows\\\\System32\\\\services.exe"},"event":{"module":"ml-hids","code":"4624","action":"An account was successfully logged on."},"input":{},"key":"rhipe_SYSTEM_2021-05-17T10:33:17.698Z","count":1,"day":"Mon","user":{"target":{"name":"SYSTEM"}},"agent":{"name":"DESKTOP-ATBQRO0","os_full":"Microsoft Windows 10 Enterprise"},"@timestamp":"2021-05-17T10:33:17.698Z","tenant":"rhipe","data":{"win":{"eventdata":{"logonType":"5"},"system":{"computer":"DESKTOP-ATBQRO0"}}},"hour":"10"}}
{"_index":"td-ml-hids-4624-2021","_type":"_doc","_id":"6TTjeXkBBGrFFIuyJ0x_","_score":1,"_source":{"process":{"name":"C:\\\\Windows\\\\System32\\\\services.exe"},"event":{"module":"ml-hids","code":"4624","action":"An account was successfully logged on."},"input":{},"key":"rhipe_SYSTEM_2021-05-17T10:33:19.395Z","count":1,"day":"Mon","user":{"target":{"name":"SYSTEM"}},"agent":{"name":"DESKTOP-QBNB8UP","os_full":"Microsoft Windows 10 Enterprise"},"@timestamp":"2021-05-17T10:33:19.395Z","tenant":"rhipe","data":{"win":{"eventdata":{"logonType":"5"},"system":{"computer":"DESKTOP-QBNB8UP"}}},"hour":"10"}}
```
