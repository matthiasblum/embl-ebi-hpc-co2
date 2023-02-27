# EMBL-EBI HPC CO2

Tracking the carbon footprint of EMBL-EBI's High Performance Computing cluster

## Track LSF jobs

```sh
python track-jobs.py /path/to/jobs.database
```

## List jobs

```sh
python view-jobs.py [options] /path/to/jobs.database
```

Options:
  * `--from YYYY-MM-DD HH:MM:SS`: ignore jobs before date/time
  * `--to YYYY-MM-DD HH:MM:SS`: ignore jobs after date/time
  * `-u USER`, `--user USER`: only list jobs of `USER`

## Track usage

```sh
python track-usage.py [options] /path/to/jobs.database /path/to/usage.database
```

Options:
  * `--from auto|today|yesterday|YYYY-MM-DD`: ignore jobs before date
    * `auto`: last time jobs were processes minus one day (default)
    * `today`: current day
    * `yesterday`: previous day
    * `YYYY-MM-DD`: specific day
  * `--to YYYY-MM-DD`: ignore jobs after date
  * `--verbose`: show progress
  * `--update-users`: update users metadata using EBI Search
  * `--users FILE`: JSON file containing user-team mappings
  * `--workers INT`: number of processing cores

The script lists unknown users (to be manually added the JSON file) and the UNIX groups to which they belong.
To list users belonging to one group, run the following command:

```sh
getent group <name>
```

## Create monthly report

```sh
python create-report.py [--verbose] /path/to/jobs.database MONTH /path/to/usage.database
```

`MONTH` is either:
  * `previous`
  * `current`
  * a given month as `YYYY-MM`
