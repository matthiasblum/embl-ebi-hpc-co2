import sys
from argparse import ArgumentParser
from datetime import datetime

from ebihpc import jobdb
from ebihpc import lsf
from ebihpc.model import UnixUser


def main():
    parser = ArgumentParser(description="Monitor jobs")
    parser.add_argument("-s", "--scheduler", default="lsf",
                        choices=["lsf", "slurm"],
                        help="job scheduler")
    parser.add_argument("database", help="job database")
    args = parser.parse_args()

    if args.scheduler == "lsf":
        jobs = lsf.get_jobs()
    elif args.scheduler == "slurm":
        raise NotImplementedError(args.scheduler)
    else:
        raise NotImplementedError(args.scheduler)

    con = jobdb.connect(args.database)
    users = jobdb.get_users(con)

    complete = []
    incomplete = []
    for job in jobs:
        if job.user not in users:
            users[job.user] = UnixUser(job.user)
            users[job.user].init()

        if job.finish_time is not None:
            complete.append(job)
        else:
            incomplete.append(job)

    jobdb.update_jobs(con, complete)
    jobdb.update_incompletes(con, incomplete)
    jobdb.update_users(con, list(users.values()))

    sys.stderr.write(f"{datetime.now():%Y-%m-%d %H:%M:%S}: "
                     f"{len(incomplete):,} jobs pending or running, "
                     f"{len(jobs):,} jobs updated\n")


if __name__ == "__main__":
    main()
