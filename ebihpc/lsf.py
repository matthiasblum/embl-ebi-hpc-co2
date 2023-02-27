import json
import re
import subprocess as sp
import sys
import time
from datetime import datetime

from . import model


REG_MEM_G = re.compile(r"(\d+(?:\.\d+)?) G(?:bytes)?")
REG_MEM_M = re.compile(r"(\d+) M(?:bytes)?")
REG_MEM_T = re.compile(r"(\d+(?:\.\d+)?) T(?:bytes)?")
REG_DATE = re.compile(r"([A-Z][a-z]{2})\s{1,2}(\d{1,2}) (\d\d:\d\d)(?: [ELX])?")


def get_jobs() -> list[model.Job]:
    fields = [
        "jobid",
        "jobindex",
        "job_name",
        "stat",
        "user",
        "queue",
        "slots",
        "memlimit",
        "max_mem",
        "from_host",
        "exec_host",
        "submit_time",
        "start_time",
        "finish_time",
        "cpu_efficiency",
        "mem_efficiency",
        "cpu_used"
    ]
    args = ["bjobs", "-u", "all", "-a", "-json", "-o", " ".join(fields)]

    while True:
        try:
            p = sp.run(args, check=True, stdout=sp.PIPE, stderr=sp.PIPE)
        except sp.CalledProcessError as e:
            sys.stderr.write(f"Command {args} failed: {e.stderr}\n")
            time.sleep(5)
        else:
            out = p.stdout.decode("utf-8", "ignore")
            break

    bjobs = json.loads(out.strip())

    jobs = []
    for rec in bjobs["RECORDS"]:
        m = re.match(r"(\d+\.\d+) second", rec["CPU_USED"])
        cpu_time = float(m.group(1)) if m else None

        j = model.Job(
            scheduler="lsf",
            id=int(rec["JOBID"]),
            index=int(rec["JOBINDEX"]),
            name=rec["JOB_NAME"],
            status=rec["STAT"],
            user=rec["USER"],
            queue=rec["QUEUE"],
            slots=int(rec["SLOTS"]) if rec["SLOTS"] else 1,
            cpu_efficiency=parse_percent(rec["CPU_EFFICIENCY"]),
            mem_lim=parse_memory(rec["MEMLIMIT"]),
            mem_max=parse_memory(rec["MAX_MEM"]),
            mem_efficiency=parse_percent(rec["MEM_EFFICIENCY"]),
            from_host=rec["FROM_HOST"],
            exec_host=rec["EXEC_HOST"] or None,
            submit_time=parse_time(rec["SUBMIT_TIME"]),
            start_time=parse_time(rec["START_TIME"]),
            finish_time=(parse_time(rec["FINISH_TIME"])
                         if rec["STAT"] in ("DONE", "EXIT") else None),
            cpu_time=cpu_time,
        )
        jobs.append(j)

    return jobs


def parse_percent(string: str) -> float | None:
    string = string.strip().replace("%", "")
    return float(string) if string else None


def parse_memory(string: str) -> int | None:
    if not string:
        return None

    m = REG_MEM_T.fullmatch(string)
    if m:
        return int(float(m.group(1)) * 1024 * 1024)

    m = REG_MEM_G.fullmatch(string)
    if m:
        return int(float(m.group(1)) * 1024)

    m = REG_MEM_M.fullmatch(string)
    if m:
        return int(m.group(1))

    raise NotImplementedError(string)


def parse_time(string) -> datetime | None:
    if not string:
        return None

    m = REG_DATE.fullmatch(string)
    if m:
        now = datetime.now()

        month, day, _time = m.groups()
        if len(day) == 1:
            day = f"0{day}"

        date_string = f"{now.year} {month} {day} {_time}"
        dt = datetime.strptime(date_string, "%Y %b %d %H:%M")

        if dt > now:
            date_string = f"{now.year - 1} {month} {day} {_time}"
            dt = datetime.strptime(date_string, "%Y %b %d %H:%M")

        return dt

    raise NotImplementedError(string)
