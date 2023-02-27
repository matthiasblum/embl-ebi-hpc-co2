import json
import pickle
import sqlite3
from datetime import datetime

from .model import UnixUser, User, DT_REPR


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
