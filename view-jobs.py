from argparse import ArgumentParser
from datetime import datetime, timedelta

from ebihpc import jobdb


def main():
    parser = ArgumentParser(description="List LSF jobs")
    parser.add_argument("--from", dest="start_time",
                        metavar="YYYY-MM-DD HH:MM:SS",
                        help="select jobs running after the specified time")
    parser.add_argument("--to", dest="end_time",
                        metavar="YYYY-MM-DD HH:MM:SS",
                        help="select jobs running before the specified time")
    parser.add_argument("-u", "--user", metavar="USER",
                        help="list USER's jobs only")
    parser.add_argument("database", help="database file")
    args = parser.parse_args()

    today = datetime.today()
    today = datetime(today.year, today.month, today.day)
    date_fmt = "%Y-%m-%d %H:%M:%S"

    if args.start_time:
        from_dt = datetime.strptime(args.start_time, date_fmt)
    else:
        from_dt = today

    if args.end_time:
        to_dt = datetime.strptime(args.end_time, date_fmt)
    else:
        to_dt = today + timedelta(days=1)

    print("\t".join([
        "#ID",
        "Status",
        "User",
        "Queue",
        "CPUs",
        "CPU efficiency",
        "Mem. limit",
        "Max mem. used",
        "Submit time",
        "Start time",
        "Finish time"
    ]))

    con = jobdb.connect(args.database)
    for job in jobdb.find_jobs(con, from_dt, to_dt, args.user):
        row = [f"{job.id}[{job.index}]", job.status, job.user, job.queue,
               job.slots, job.cpu_efficiency, job.mem_lim, job.mem_max,
               strftime(job.submit_time, date_fmt),
               strftime(job.start_time, date_fmt),
               strftime(job.finish_time, date_fmt)]
        row = [v if v is not None else "-" for v in row]
        print("\t".join(map(str, row)))

    con.close()


def strftime(dt: datetime | None, fmt: str) -> str:
    if dt:
        return dt.strftime(fmt)
    return "-"


if __name__ == "__main__":
    main()
