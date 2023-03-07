import bisect
import json
import logging
import math
import os
import pickle
from argparse import ArgumentParser
from concurrent.futures import as_completed, ProcessPoolExecutor
from datetime import datetime, timedelta
from tempfile import mkstemp

from ebihpc import const
from ebihpc import jobdb
from ebihpc import usagedb
from ebihpc.model import User


RUNTIMES = [60, 600, 3600, 3 * 3600, 6 * 3600, 12 * 3600, 24 * 3600,
            48 * 3600, 72 * 3600, 7 * 24 * 3600]


def main():
    parser = ArgumentParser(description="Calculate carbon footprint of jobs")
    parser.add_argument("--from", dest="from_time", default="auto",
                        metavar="auto|today|yesterday|YYYY-MM-DD")
    parser.add_argument("--to", dest="to_time", metavar="YYYY-MM-DD")
    parser.add_argument("--verbose", action="store_true", help="show progress")
    parser.add_argument("--update-users", choices=["yes", "no"], default="yes",
                        help="if 'yes', update users metadata")
    parser.add_argument("--users", help="JSON file of custom users metadata")
    parser.add_argument("--workers", type=int, default=1,
                        help="number of workers")
    parser.add_argument("input", help="job database")
    parser.add_argument("output", help="usage database")
    args = parser.parse_args()

    logging.basicConfig(format="%(asctime)s    %(levelname)s:    %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S",
                        level=logging.DEBUG if args.verbose else logging.INFO)

    logging.info("Loading users")
    con = jobdb.connect(args.input)
    unix_users = jobdb.get_users(con)
    last_jobs_update = jobdb.get_latest_update_time(con)
    con.close()

    # Load (and update) users
    con = usagedb.connect(args.output)
    users = {}
    for user in usagedb.get_users(con, unix_users):
        users[user.login] = user
        if args.update_users == "yes":
            user.update()

    # Add Unix users
    for unix_user in unix_users.values():
        if unix_user.login not in users:
            user = User(login=unix_user.login,
                        group=unix_user.group,
                        groups=unix_user.groups)
            user.update()
            users[user.login] = user

    # Override existing users with custom ones
    if args.users:
        with open(args.users, "rt") as fh:
            custom_users = json.load(fh)

        for login, meta in custom_users.items():
            try:
                user = users[login]
            except KeyError:
                user = users[login] = User(login)
                user.update()

            if meta["name"]:
                if user.name and user.name != meta["name"]:
                    logging.warning(f"{login}: {meta['name']} ≠ "
                                    f"{user.name} (name)")
                user.name = meta["name"]

            if meta["position"]:
                if user.position and user.position != meta["position"]:
                    logging.warning(f"{login}: {meta['position']} ≠ "
                                    f"{user.position} (position)")
                user.position = meta["position"]

            if meta["teams"]:
                if user.teams and user.teams != meta["teams"]:
                    logging.warning(f"{login}: {', '.join(meta['teams'])} ≠ "
                                    f"{', '.join(user.teams)} (teams)")
                user.teams = meta["teams"]

            if meta["sponsor"]:
                if user.sponsor and user.sponsor != meta["sponsor"]:
                    logging.warning(f"{login}: {meta['sponsor']} ≠ "
                                    f"{user.sponsor} (sponsor)")
                user.sponsor = meta["sponsor"]
    else:
        custom_users = {}

    for user in users.values():
        if not user.teams:
            s = " (custom) " if user.login in custom_users else " "
            logging.warning(f"{user.login}{s}is not in any team "
                            f"(groups: {user.groups or 'N/A'})")

    user2index = {}
    for i, user in enumerate(users.values()):
        user2index[user.login] = i

    if args.from_time == "auto":
        dt = usagedb.get_latest_update_time(con, "usage") - timedelta(days=1)
        from_time = datetime(dt.year, dt.month, dt.day)
    elif args.from_time == "today":
        dt = datetime.today()
        from_time = datetime(dt.year, dt.month, dt.day)
    elif args.from_time == "yesterday":
        dt = datetime.today() - timedelta(days=1)
        from_time = datetime(dt.year, dt.month, dt.day)
    else:
        from_time = datetime.strptime(args.from_time, "%Y-%m-%d")

    if args.to_time:
        to_time = datetime.strptime(args.to_time, "%Y-%m-%d")
    else:
        dt = datetime.today() + timedelta(days=1)
        to_time = datetime(dt.year, dt.month, dt.day)

    logging.info("Processing jobs")
    with ProcessPoolExecutor(max_workers=max(1, args.workers)) as executor:
        fs = {}
        for dt in usagedb.range_dt(from_time, to_time, timedelta(days=1)):
            dt2 = dt + timedelta(days=1)
            f = executor.submit(process_jobs, args.input, dt, dt2, user2index)
            fs[f] = dt.strftime("%Y-%m-%d")

        for f in as_completed(fs):
            output, num_jobs = f.result()
            usagedb.update_usage(con, output)
            os.unlink(output)
            logging.info(f"{fs[f]}: {num_jobs:,} jobs processed")

    usagedb.bump_update_times(con, last_jobs_update)
    usagedb.update_users(con, list(users.values()))
    con.close()
    logging.info("Done")


def process_jobs(database: str, from_dt: datetime, to_dt: datetime,
                 user2index: dict[str, int]) -> tuple[str, int]:
    # Stats in intervals of one minute
    job_intervals = list(usagedb.range_dt(from_dt, to_dt, timedelta(minutes=1)))
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
    final_intervals = list(usagedb.range_dt(from_dt, to_dt,
                                            timedelta(minutes=15)))
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
        mem_eff = min(job.mem_efficiency, 100)

        cores_power = job.slots * (cpu_eff / 100) * const.CPU_POWER
        if "gpu" in job.queue:
            # Unknown GPU number and GPU efficiency: assume 1
            cores_power += 1 * 1 * const.GPU_POWER

        use_mem_eff = False
        if job.mem_lim is not None:
            mem_gb = job.mem_lim / 1024
            use_mem_eff = True
        elif job.mem_max is not None:
            mem_gb = job.mem_max / 1024
        else:
            mem_gb = 0

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
            runtime = (job.start_time - finish_time).total_seconds()
            co2e, cost = const.calc_footprint(energy_kw, runtime / 3600)

            user_data = users_extra_data[i][j]
            job_data = jobs_data[i]
            if job.ok:
                user_data["done"] += 1

                if use_mem_eff:
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
                if use_mem_eff:
                    j = min(math.floor(mem_eff), 99)
                    job_data["memeff"]["dist"][j] += 1

                job_data["cpueff"][min(math.floor(cpu_eff), 99)] += 1

                for x, maxtime in enumerate(RUNTIMES):
                    if runtime <= maxtime:
                        job_data["runtimes"][x] += 1
                        break
                else:
                    job_data["runtimes"][-1] += 1

                if use_mem_eff:
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


if __name__ == '__main__':
    main()
