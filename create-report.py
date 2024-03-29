import logging
import math
from argparse import ArgumentParser
from datetime import datetime, timedelta

from ebihpc import const, jobdb, usagedb


DT_REPR = "%Y-%m-%d %H:%M:%S"


def main():
    parser = ArgumentParser(description="Create monthly report")
    parser.add_argument("--verbose", action="store_true", help="show progress")
    parser.add_argument("input", help="job database")
    parser.add_argument("month", metavar="current|previous|YYYY-MM")
    parser.add_argument("output", help="usage database")
    args = parser.parse_args()

    logging.basicConfig(format="%(asctime)s    %(levelname)s:    %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S",
                        level=logging.DEBUG if args.verbose else logging.INFO)

    dt = datetime.today()
    if args.month == "current":
        from_time = datetime(dt.year, dt.month, 1)
    elif args.month == "previous":
        if dt.month > 1:
            from_time = datetime(dt.year, dt.month - 1, 1)
        else:
            from_time = datetime(dt.year - 1, 12, 1)
    else:
        from_time = datetime.strptime(args.month, "%Y-%m")

    if from_time.month < 12:
        to_time = datetime(from_time.year, from_time.month + 1, 1)
    else:
        to_time = datetime(from_time.year + 1, 1, 1)

    logging.info(f"Creating report for {from_time:%B %Y}")

    con = jobdb.connect(args.input)
    last_jobs_update = jobdb.get_latest_update_time(con)
    con.close()

    user_data = {}
    num_jobs = 0
    for job in jobdb.find_jobs(args.input, from_time, to_time):
        num_jobs += 1

        if num_jobs % 1e6 == 0:
            logging.debug(f"{num_jobs:>20,}")

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
            finish_time = min(last_jobs_update, to_time)
        elif start_time == finish_time:
            # One minute or less
            finish_time += timedelta(minutes=1)

        runtime_min = (finish_time - start_time).total_seconds() / 60
        energy_kw = (cores_power + mem_power) / 1000
        co2e, cost = const.calc_footprint(energy_kw, runtime_min / 60,
                                          start_time)
        minutes = 0
        for dt in usagedb.range_dt(start_time,
                                   finish_time,
                                   timedelta(minutes=1)):
            if dt >= to_time:
                break
            elif from_time <= dt:
                minutes += 1

        try:
            data = user_data[job.user]
        except KeyError:
            data = user_data[job.user] = {
                "jobs": {
                    "total": 0,
                    "done": 0,
                    "exit": 0
                },
                "co2e": 0,
                "cost": 0,
                "memory": [0] * 100,
                "cputime": 0,
                "rank": None,
                "totalCo2e": 0
            }

        data["jobs"]["total"] += 1
        if job.finish_time:
            if job.ok:
                data["jobs"]["done"] += 1

                if (mem_eff is not None and
                        mem_lim is not None and
                        mem_lim >= const.MIN_MEM_REQ):
                    data["memory"][min(math.floor(mem_eff), 99)] += 1
            else:
                data["jobs"]["exit"] += 1

        data["co2e"] += co2e / runtime_min * minutes
        data["cost"] += cost / runtime_min * minutes
        data["cputime"] += job.cpu_time or 0

    con.close()

    logging.debug(f"{num_jobs:>20,}")

    total_co2e = sum((u["co2e"] for u in user_data.values()))

    for i, user in enumerate(sorted(user_data.values(),
                                    key=lambda u: -u["co2e"])):
        user["rank"] = i + 1
        user["totalCo2e"] = total_co2e

    usagedb.update_reports(args.output, from_time, user_data)
    logging.info("Done")


if __name__ == '__main__':
    main()
