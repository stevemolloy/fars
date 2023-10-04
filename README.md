# fars
This tool allows the fast extraction of data from the Fast Archiver system developed by Diamond.

# Downloading
The [releases](https://github.com/stevemolloy/fars/releases) page provides links to download the source code, as well as a link to a precompiled Linux executable (`fars`) and a Windows executable (`fars.exe`).

# Use
This tool is designed for use at the MAX-IV accelerator complex, and so needs at least three things to be specified on the command line.
- The accelerator ring for which the data is needed.  This can be `R1` or `R3`.
- The start date & time in the format `yyyy-mm-ddTHH:MM:SS.sss`.
- The end date & time in the same format.

Data will be saved as multiple files (one for each BPM) in the format `fa_dataxxx.dat`, where `xxx` is a three digit number representing the BPM in question.

## An example
The following will grab 10 seconds of data for all BPMs in R3.
`fars --ring R3 --start 2023-10-04T12:00:00 --end 2023-10-04T12:00:10`

## Specific BPMs
Providing a string as a cli parameter will cause the list of BPMs for the ring in question to be searched with a regular expression of the form `^searchterm$`, where `searchterm` is the string provided as input.

In addition, using the name of a MAXIV beamline as input (e.g., `danmax`, `maxpeem`, `mik`, etc.) will provide data for the two BPMs that flank the ID associated with that beamline.

## Changing the behaviour
The flag `--find_dump` alers the behaviour of this tool quite significantly.

## Additional flags

`--deci` -- This will request data from the decimated stream.
`--file filename` -- This will change the filename to `filenamexxx.dat`.
