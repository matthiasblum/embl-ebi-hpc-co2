import json
import sqlite3
from argparse import ArgumentParser
from datetime import datetime, timedelta

from ebihpc import usagedb


def iter_usage(con: sqlite3.Connection, start: datetime, stop: datetime):
    for dt_str, raw_data in con.execute(
        """
            SELECT time, users_data 
            FROM usage
            WHERE time >= ? AND time < ?
            ORDER BY time
        """,
        [start.strftime(usagedb.DT_FMT), stop.strftime(usagedb.DT_FMT)]
    ):
        dt = datetime.strptime(dt_str, usagedb.DT_FMT)
        yield dt, json.loads(raw_data)


def main():
    parser = ArgumentParser(description="View job usage")
    parser.add_argument("--from", dest="from_time", metavar="YYYY-MM-DD",
                        help="Beginning of interval to consider")
    parser.add_argument("--to", dest="to_time", metavar="YYYY-MM-DD",
                        help="End of interval to consider")
    parser.add_argument("--interval", choices=["day", "week", "month"],
                        default="day", help="Group usage by day, "
                                            "week, or month (default: day)")
    parser.add_argument("--by-team", action="store_true",
                        help="Group usage by team (default: disabled)")
    parser.add_argument("--num-series", type=int, default=0,
                        help="Max number of series to show if --by-team "
                             "is enabled (default: all)")
    parser.add_argument("--users", nargs="+",
                        help="List of users (default: all users)")
    parser.add_argument("--unit", choices=["g", "kg", "t"], default="kg",
                        help="Unit of CO2-equivalent (default: kg)")
    parser.add_argument("--database", required=True, help="Usage database")
    args = parser.parse_intermixed_args()

    con = sqlite3.connect(args.database)

    users = {}
    for user in usagedb.get_users(con):
        users[user.login] = user.teams if args.by_team else ["EMBL-EBI"]

    if args.users:
        users = {k: v for k, v in users.items() if k in args.users}

    dt_fmt = {
        "day": "%Y-%m-%d",
        "week": "%Y-%W",
        "month": "%Y-%m"
    }[args.interval]

    if args.from_time:
        start = datetime.strptime(args.from_time, "%Y-%m-%d")
    else:
        dt_str, = con.execute("SELECT MIN(time) FROM usage").fetchone()
        start = datetime.strptime(dt_str, usagedb.DT_FMT)

    if args.to_time:
        stop = datetime.strptime(args.to_time, "%Y-%m-%d")
    else:
        dt_str, = con.execute("SELECT MAX(time) FROM usage").fetchone()
        stop = datetime.strptime(dt_str, usagedb.DT_FMT)

    usage = {}
    for dt in usagedb.range_dt(start, stop, timedelta(days=1)):
        dt_str = dt.strftime(dt_fmt)
        usage[dt_str] = {}

    total = {}
    for dt, users_data in iter_usage(con, start, stop):
        dt_str = dt.strftime(dt_fmt)

        for user in users_data:
            if user in users:
                co2e = users_data[user]["co2e"]

                try:
                    teams_usage = usage[dt_str]
                except KeyError:
                    teams_usage = usage[dt_str] = {}

                for team in users[user]:
                    try:
                        teams_usage[team] += co2e
                    except KeyError:
                        teams_usage[team] = total[team] = co2e
                    else:
                        total[team] += co2e

    teams = sorted(total, key=lambda k: -total[k])

    if args.num_series > 0:
        if len(teams) > args.num_series:
            has_others = True
            teams = teams[:max(1, args.num_series-1)]
        else:
            has_others = False
    else:
        has_others = False

    if has_others:
        print(f"Time", '\t'.join(teams), "Others", sep="\t")
    else:
        print(f"Time", '\t'.join(teams), sep="\t")

    factor, ndigits = {
        "g": (1, 0),
        "kg": (1e-3, 0),
        "t": (1e-6, 3)
    }[args.unit]

    for dt_str in sorted(usage):
        row = [dt_str]

        for team in teams:
            co2e = usage[dt_str].pop(team, 0)
            row.append(str(round(co2e * factor, ndigits)))

        if has_others:
            co2e = sum(usage[dt_str].values())
            row.append(str(round(co2e * factor, ndigits)))

        print("\t".join(row))

    con.close()


if __name__ == '__main__':
    main()
