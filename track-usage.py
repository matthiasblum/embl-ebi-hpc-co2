import json
import logging
import os
from argparse import ArgumentParser
from concurrent.futures import as_completed, ProcessPoolExecutor
from datetime import datetime, timedelta

from ebihpc import jobdb
from ebihpc import usagedb
from ebihpc.model import User


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
            f = executor.submit(usagedb.process_jobs,
                                args.input, dt, dt2, user2index)
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


if __name__ == '__main__':
    main()
