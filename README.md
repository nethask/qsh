# QSH

This library makes it easy to read QSH binary file format of version 4 using Python.

Project in a stage of active development. Contribution are welcomed!

Supported file formats:
- Plain binary file - not tested
- Compressed with gzip binary file

Supported data streams:
- Quotes
- Deals
- OwnOrders - not tested
- OwnTrades - not tested
- Messages  - not tested
- AuxInfo
- OrdLog

Links to QSH archives:

- [http://finam.qscalp.ru/](http://finam.qscalp.ru/)
- [ftp://zerich.qscalp.ru/](ftp://zerich.qscalp.ru/)
- [http://qsh.qscalp.ru/](http://qsh.qscalp.ru/)

## Install

```
pip install qsh
```

## Usage

For case of **one stream** in a file

```
import qsh

with qsh.open("Si-9.18.2018-08-24.OrdLog.qsh") as qsh_file:
    print("=== Header ===")
    print("Signature: "     + str(qsh_file.header.signature))
    print("Version: "       + str(qsh_file.header.version))
    print("Application: "   + str(qsh_file.header.application))
    print("Comment: "       + str(qsh_file.header.comment))
    print("Created at: "    + str(qsh_file.header.created_at))
    print("Streams count: " + str(qsh_file.header.streams_count))

    # If file contains one stream
    stream_type, instrument_code = qsh_file.read_stream_header()

    # Read frame header & frame data for one stream case
    try:
        while True:
            frame_timestamp, _ = qsh_file.read_frame_header()
            ord_log_entry, aux_info_entry, updated_quotes, deal_entry = qsh_file.read_ord_log_stream()

            # Do something...
    except EOFError:
        pass
```

For case of **more than one stream** in a file

```
import qsh

streams = {}
with qsh.open("Si-9.18.2018-08-24.OrdLog.qsh") as qsh_file:
    # If file contains two or more streams
    for i in range(qsh_file.header.streams_count):
        stream_type, instrument_code = qsh_file.read_stream_header()
        streams[i] = {}
        streams[i]["type"] = stream_type
        streams[i]["intrument_code"] = instrument_code

    # Read frame header & frame data for more than one stream case
    try:
        while True:
            frame_timestamp, stream_index = qsh_file.read_frame_header()
            
            if streams[stream_index]["type"] == qsh.StreamType.ORD_LOG:
                pass # Do something...
    except EOFError:
        pass
```

By default all timestamps are in UTC timezone so to convert them to local timezone you can use something like

```
from dateutil import tz

from_zone = tz.gettz("UTC")
to_zone   = tz.gettz("Europe/Moscow")

# Get ord_log_entry from file and then

ord_log_entry.exchange_timestamp.replace(tzinfo=from_zone).astimezone(to_zone)
```
