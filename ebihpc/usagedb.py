import bisect
import json
import logging
import math
import pickle
import sqlite3
from datetime import datetime, timedelta
from tempfile import mkstemp

from . import const, jobdb
from .model import UnixUser, User, DT_REPR


RUNTIMES = [60, 600, 3600, 3 * 3600, 6 * 3600, 12 * 3600, 24 * 3600,
            48 * 3600, 72 * 3600, 7 * 24 * 3600]


def connect(database: str) -> sqlite3.Connection:
    con = sqlite3.connect(database)
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS usage (
            time TEXT PRIMARY KEY NOT NULL,
            users_data BLOB NOT NULL,
            jobs_data BLOB NOT NULL
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS user (
            login TEXT PRIMARY KEY NOT NULL,
            name TEXT,
            uuid TEXT NOT NULL,
            teams TEXT NOT NULL,
            position TEXT,
            photo_url TEXT,
            sponsor TEXT
        )
        """
    )
    con.execute("CREATE UNIQUE INDEX IF NOT EXISTS user_uuid ON user (uuid)")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS metadata (
            key TEXT PRIMARY KEY NOT NULL,
            value TEXT NOT NULL
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS report (
            login TEXT NOT NULL,
            month TEXT NOT NULL,
            data TEXT NOT NULL,
            CONSTRAINT pk_report PRIMARY KEY (login, month)
        )
        """
    )
    return con


def get_users(con: sqlite3.Connection,
              unix_users: dict[str, UnixUser]) -> list[User]:
    users = []
    for row in con.execute("SELECT * FROM user").fetchall():
        login, name, uuid, teams, position, photo_url, sponsor = row
        try:
            unix_user = unix_users[login]
        except KeyError:
            group = groups = None
        else:
            group = unix_user.group
            groups = unix_user.groups

        user = User(login=login,
                    group=group,
                    groups=groups,
                    name=name,
                    teams=json.loads(teams),
                    position=position,
                    photo_url=photo_url,
                    uuid=uuid,
                    sponsor=sponsor)
        users.append(user)

    return users


def get_latest_update_time(con: sqlite3.Connection, datatype: str) -> datetime:
    if datatype not in ["jobs", "usage"]:
        raise ValueError(datatype)

    date_str, = con.execute("SELECT value FROM metadata "
                            "WHERE key =?", [datatype]).fetchone()
    return datetime.strptime(date_str, DT_REPR)


def bump_update_times(con: sqlite3.Connection, jobs_update_time: datetime):
    sql = "INSERT OR REPLACE INTO metadata VALUES (?, ?)"
    params = [
        ["jobs", jobs_update_time.strftime(DT_REPR)],
        ["usage", datetime.today().strftime(DT_REPR)]
    ]
    con.executemany(sql, params)
    con.commit()


def update_users(con: sqlite3.Connection, users: list[User]):
    sql = "INSERT OR REPLACE INTO user VALUES (?, ?, ?, ?, ?, ?, ?)"
    con.executemany(sql, (u.to_tuple() for u in users))
    con.commit()


def update_usage(con: sqlite3.Connection, file: str):
    sql = "INSERT OR REPLACE INTO usage VALUES (?, ?, ?)"
    con.executemany(sql, _parse_output(file))
    con.commit()


def _parse_output(file: str):
    with open(file, "rb") as fh:
        while True:
            try:
                key, data, other_data = pickle.load(fh)
            except EOFError:
                break
            else:
                yield key, json.dumps(data), json.dumps(other_data)


def update_reports(database: str, dt: datetime, data: dict[str, dict]):
    month = dt.strftime("%Y-%m")

    con = connect(database)
    con.executemany("INSERT OR REPLACE INTO report VALUES (?, ?, ?)",
                    ((uname, month, json.dumps(user_data))
                     for uname, user_data in data.items())
                    )
    con.commit()
    con.close()


def range_dt(start: datetime, stop: datetime, step: timedelta):
    while start < stop:
        yield start
        start += step


def process_jobs(database: str, from_dt: datetime, to_dt: datetime,
                 user2index: dict[str, int]) -> tuple[str, int]:
    # Stats in intervals of one minute
    job_intervals = list(range_dt(from_dt, to_dt, timedelta(minutes=1)))
    users_data = []
    for _ in job_intervals:
        obj = []
        for _ in user2index:
            obj.append({
                "jobs": 0,
                "cores": 0,
                "memory": 0,
                "co2e": 0,
                "cost": 0,
                "cputime": 0
            })

        users_data.append(obj)

    # Stats in intervals of 15 minutes
    final_intervals = list(range_dt(from_dt, to_dt, timedelta(minutes=15)))
    jobs_data = []
    users_extra_data = []
    for _ in final_intervals:
        obj = []
        for _ in user2index:
            obj.append({
                "submitted": 0,
                "done": 0,
                "failed": 0,
                "memeff": [0] * 5,
                "cpueff": [0] * 5,
            })

        users_extra_data.append(obj)

        jobs_data.append({
            "done": 0,
            "cpueff": [0] * 100,
            "runtimes": [0] * (len(RUNTIMES) + 1),
            "memeff": {
                "dist": [0] * 100,
                "co2e": 0,
                "cost": 0
            },
            "failed": {
                "count": 0,
                "co2e": 0,
                "cost": 0
            }
        })

    con = jobdb.connect(database)
    last_jobs_update = jobdb.get_latest_update_time(con)
    num_jobs = 0
    label = f"{from_dt:%Y-%m-%d} - {to_dt:%Y-%m-%d}"
    for job in jobdb.find_jobs(con, from_dt, to_dt):
        num_jobs += 1

        if num_jobs % 1e5 == 0:
            logging.debug(f"{label}: {num_jobs:>20,}")

        cpu_eff = min(job.cpu_efficiency, 100)
        cores_power = job.slots * (cpu_eff / 100) * const.CPU_POWER
        if "gpu" in job.queue:
            # Unknown GPU number and GPU efficiency: assume 1
            cores_power += 1 * 1 * const.GPU_POWER

        mem_lim, mem_max, mem_eff = job.fix_mem()
        mem_gb = (mem_lim or mem_max or 0) / 1024
        mem_power = mem_gb * const.MEM_POWER

        start_time = job.start_time
        finish_time = job.finish_time
        if finish_time is None:
            finish_time = min(last_jobs_update, to_dt)
        elif start_time == finish_time:
            # One minute or less
            finish_time += timedelta(minutes=1)

        # Runtime of the job
        runtime_min = (finish_time - start_time).total_seconds() / 60
        energy_kw = (cores_power + mem_power) / 1000
        co2e, cost = const.calc_footprint(energy_kw, runtime_min / 60)
        cpu_time = job.cpu_time or 0

        # Move start_time to beginning of interval of interest
        while start_time < from_dt:
            start_time += timedelta(minutes=1)

        # Update user data for every interval of 15min during which the job ran
        i = bisect.bisect_left(job_intervals, start_time)
        j = user2index[job.user]
        while i < len(job_intervals) and job_intervals[i] < finish_time:
            user_data = users_data[i][j]
            user_data["jobs"] += 1 / runtime_min
            user_data["cores"] += job.slots
            user_data["memory"] += mem_gb
            user_data["co2e"] += co2e / runtime_min
            user_data["cost"] += cost / runtime_min
            user_data["cputime"] += cpu_time / runtime_min
            i += 1

        if job.submit_time >= from_dt:
            i = bisect.bisect_right(final_intervals, job.submit_time) - 1
            if i < 0:
                raise ValueError

            # Record job as submitted in this interval
            users_extra_data[i][j]["submitted"] += 1

        if job.finish_time and finish_time < to_dt:
            # Record job as completed in this interval
            i = bisect.bisect_right(final_intervals, finish_time) - 1
            if i < 0:
                raise ValueError

            # Footprint of entire job
            runtime = (finish_time - job.start_time).total_seconds()
            co2e, cost = const.calc_footprint(energy_kw, runtime / 3600)

            user_data = users_extra_data[i][j]
            job_data = jobs_data[i]
            if job.ok:
                user_data["done"] += 1

                if mem_eff is not None:
                    if mem_eff < 20:
                        user_data["memeff"][0] += 1
                    elif mem_eff < 40:
                        user_data["memeff"][1] += 1
                    elif mem_eff < 60:
                        user_data["memeff"][2] += 1
                    elif mem_eff < 80:
                        user_data["memeff"][3] += 1
                    else:
                        user_data["memeff"][4] += 1

                if cpu_eff < 20:
                    user_data["cpueff"][0] += 1
                elif cpu_eff < 40:
                    user_data["cpueff"][1] += 1
                elif cpu_eff < 60:
                    user_data["cpueff"][2] += 1
                elif cpu_eff < 80:
                    user_data["cpueff"][3] += 1
                else:
                    user_data["cpueff"][4] += 1

                job_data["done"] += 1
                if mem_eff is not None:
                    j = min(math.floor(mem_eff), 99)
                    job_data["memeff"]["dist"][j] += 1

                job_data["cpueff"][min(math.floor(cpu_eff), 99)] += 1

                for x, maxtime in enumerate(RUNTIMES):
                    if runtime <= maxtime:
                        job_data["runtimes"][x] += 1
                        break
                else:
                    job_data["runtimes"][-1] += 1

                if mem_eff is not None:
                    # Footprint of entire job with good memory efficiency (+10%)
                    opti_mem = (mem_gb * mem_eff / 100) * 1.1
                    mem_power = opti_mem * const.MEM_POWER
                    energy_kw = (cores_power + mem_power) / 1000
                    values = const.calc_footprint(energy_kw, runtime / 3600)
                    opti_co2e, opti_cost = values
                    job_data["memeff"]["co2e"] += (co2e - opti_co2e)
                    job_data["memeff"]["cost"] += (cost - opti_cost)
            else:
                user_data["failed"] += 1
                job_data["failed"]["count"] += 1
                job_data["failed"]["co2e"] += co2e
                job_data["failed"]["cost"] += cost

    # Merge one-minute intervals data in 15-minute intervals
    fd, output = mkstemp()
    with open(fd, "wb") as fh:
        users = sorted(user2index.keys(), key=lambda k: user2index[k])

        for i, dt in enumerate(final_intervals):
            _data = {}
            for interval_data in users_data[i * 15:(i + 1) * 15]:
                for j, values in enumerate(interval_data):
                    if values["jobs"] == 0:
                        continue

                    uname = users[j]

                    try:
                        obj = _data[uname]
                    except KeyError:
                        obj = _data[uname] = {k: 0 for k in values}
                        obj.update(users_extra_data[i][j])

                    obj["jobs"] += values["jobs"]
                    obj["cores"] = max(obj["cores"], values["cores"])
                    obj["memory"] = max(obj["memory"], values["memory"])
                    obj["co2e"] += values["co2e"]
                    obj["cost"] += values["cost"]
                    obj["cputime"] += values["cputime"]

            pickle.dump((
                dt.strftime("%Y%m%d%H%M"),
                _data,
                jobs_data[i]
            ), fh)

    return output, num_jobs
