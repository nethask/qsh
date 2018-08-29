# QSH

This library makes it possible to read QSH binary file format using Python.

Project in a stage of active development and was tested only for binary files
with gzip compression and one stream. Library for now supports only OrdLog data stream.
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
    stream_id, instrument_code = qsh_file.read_stream_header()

    # Read frame header & frame data for one stream case
    try:
        while True:
            frame_timestamp, _ = qsh_file.read_frame_header()
            ord_log_entry, aux_info_entry, quotes, deal_entry = qsh_file.read_ord_log_stream()

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
        stream_id, instrument_code = qsh_file.read_stream_header()

    # Read frame header & frame data for one stream case
    try:
        while True:
            frame_timestamp, stream_index = qsh_file.read_frame_header()

            # Use stream_index to decide which data reader to call

            # Do something...
    except EOFError:
        pass
```
