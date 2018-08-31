# QSH

This library makes it easy to read QSH binary file format using Python.

Project in a stage of active development and was tested only for binary files
with gzip compression and one stream.

Supported data streams:
- OrdLog
- Quote
- Message

Other types of streams will be added soon.

Developed for version 4 of QSH file format.

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

with qsh.open("Si-9.18.2018-08-24.OrdLog.qsh") as qsh_file:
    print("=== Header ===")
    print("Signature: "     + str(qsh_file.header.signature))
    print("Version: "       + str(qsh_file.header.version))
    print("Application: "   + str(qsh_file.header.application))
    print("Comment: "       + str(qsh_file.header.comment))
    print("Created at: "    + str(qsh_file.header.created_at))
    print("Streams count: " + str(qsh_file.header.streams_count))

    # If file contains two or more streams
    for _ in range(qsh_file.header.streams_count):
        stream_type, instrument_code = qsh_file.read_stream_header()

    # Read frame header & frame data for one stream case
    try:
        while True:
            frame_timestamp, stream_index = qsh_file.read_frame_header()

            # Use stream_index & stream_type to decide which data reader to call

            # Do something...
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
