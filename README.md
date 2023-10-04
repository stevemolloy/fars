# fars
This tool allows the fast extraction of data from the [Fast Archiver](https://github.com/dls-controls/fa-archiver) system developed by Diamond.

# Downloading
The [releases](https://github.com/stevemolloy/fars/releases) page provides links to download the source code, as well as a link to a precompiled Linux executable (`fars`) and a Windows executable (`fars.exe`).

# Building from source
This is a Rust project, so the following will create a release quality build.
```bash
cargo build --release
```

# Use
This tool is designed for use at the MAX-IV accelerator complex, and so needs at least three things to be specified on the command line.
- The accelerator ring for which the data is needed.  This can be `R1` or `R3`.
- The start date & time in the format `yyyy-mm-ddTHH:MM:SS.sss`.
- The end date & time in the same format.

Data will be saved as multiple files (one for each BPM) in the format `fa_dataxxx.dat`, where `xxx` is a three digit number representing the BPM in question.

## An example
The following will grab 10 seconds of data for all BPMs in R3.
```bash
fars --ring R3 --start 2023-10-04T12:00:00 --end 2023-10-04T12:00:10
```

## Specific BPMs
Providing a string as a cli parameter will cause the list of BPMs for the ring in question to be searched with a regular expression of the form `^searchterm$`, where `searchterm` is the string provided as input.

In addition, using the name of a MAXIV beamline as input (e.g., `danmax`, `maxpeem`, `mik`, etc.) will provide data for the two BPMs that flank the ID associated with that beamline.

## Changing the behaviour
The flag `--find_dump` alers the behaviour of this tool quite significantly.

When this flag is provided, the code will search between the `start` and `end` times provided for a dump.  The data that this call provides will be for all BPMs for the ring in question, and span a period that is from 9 seconds before the beam dump and 1 second afterwards.

The dump is located by acquiring decimated data for the given time period, and searching for the signature of a dump within this data.  This code is still relatively new, and so will crash in a relatively ugly way if there is no beam dump within the specified period.

## Additional flags

- `--deci` -- This will request data from the decimated stream.
- `--file filename` -- This will change the filename to `filenamexxx.dat`.
