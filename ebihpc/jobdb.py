import sqlite3
from datetime import datetime

from .model import Job, UnixUser, DT_REPR


def connect(database: str) -> sqlite3.Connection:
    con = sqlite3.connect(database)
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS job (
            id TEXT NOT NULL PRIMARY KEY,
            scheduler TEXT NOT NULL,
            jobid INTEGER NOT NULL,
            jobindex INTEGER NOT NULL,
            name TEXT NOT NUll,
            status TEXT NOT NULL,
            user TEXT NOT NULL,
            queue TEXT NOT NULL,
            slots INTEGER NOT NULL,
            cpu_efficiency REAL,
            cpu_time REAL,
            mem_lim INTEGER,
            mem_max INTEGER,
            mem_efficiency REAL,
            from_host TEXT NOT NULL,
            exec_host TEXT,
            submit_time TEXT NOT NULL,
            start_time TEXT,
            finish_time TEXT NOT NULL,
            update_time TEXT NULL NULL
        )
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS job_user
        ON job (user)
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS job_starttime
        ON job (start_time)
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS job_endtime
        ON job (finish_time)
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS job_startendtime
        ON job (start_time, finish_time)
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS job_updatetime
        ON job (update_time)
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS incomplete (
            id TEXT NOT NULL PRIMARY KEY,
            scheduler TEXT NOT NULL,
            jobid INTEGER NOT NULL,
            jobindex INTEGER NOT NULL,
            name TEXT NOT NUll,
            status TEXT NOT NULL,
            user TEXT NOT NULL,
            queue TEXT NOT NULL,
            slots INTEGER NOT NULL,
            cpu_efficiency REAL,
            cpu_time REAL,
            mem_lim INTEGER,
            mem_max INTEGER,
            mem_efficiency REAL,
            from_host TEXT NOT NULL,
            exec_host TEXT,
            submit_time TEXT NOT NULL,
            start_time TEXT,
            finish_time TEXT,
            update_time TEXT NULL NULL
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS user (
            login TEXT NOT NULL PRIMARY KEY,
            unix_group TEXT NOT NULL,
            unix_groups TEXT NOT NULL
        )
        """
    )
    return con


def get_incomplete(con: sqlite3.Connection):
    for row in con.execute("SELECT * FROM incomplete").fetchall():
        yield Job.from_tuple(row)


def get_users(con: sqlite3.Connection) -> dict[str, UnixUser]:
    users = {}
    for login, group, groups in con.execute("SELECT * FROM user").fetchall():
        u = UnixUser(login, group, groups)
        users[u.login] = u

    return users


def update_jobs(con: sqlite3.Connection, jobs: list[Job]):
    con.executemany(
        """
        INSERT OR REPLACE INTO job
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (job.to_tuple() for job in jobs)
    )
    con.commit()


def update_incompletes(con: sqlite3.Connection, jobs: list[Job]):
    con.execute("DELETE FROM incomplete")
    con.executemany(
        """
        INSERT INTO incomplete
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (job.to_tuple() for job in jobs)
    )
    con.commit()


def update_users(con: sqlite3.Connection, users: list[UnixUser]):
    con.executemany(
        """
        INSERT OR REPLACE INTO user
        VALUES (?, ?, ?)
        """,
        [(u.login, u.group, u.groups) for u in users]
    )
    con.commit()


def get_latest_update_time(con: sqlite3.Connection) -> datetime:
    date_str, = con.execute("SELECT MAX(update_time) FROM job").fetchone()
    return datetime.strptime(date_str, DT_REPR)


def find_jobs(con: sqlite3.Connection, from_dt: datetime, to_dt: datetime,
              user: str | None = None):
    from_time = from_dt.strftime(DT_REPR)
    to_time = to_dt.strftime(DT_REPR)

    job_params = [from_time, to_time, from_time, to_time, from_time, to_time]
    inc_params = [to_time]

    if user:
        user_filter = "AND user = ?"
        job_params.append(user)
        inc_params.append(user)
    else:
        user_filter = ""

    for row in con.execute(
        f"""
        SELECT *
        FROM job
        WHERE start_time IS NOT NULL
          AND (
            (start_time >= ? AND start_time < ?)
            OR
            (finish_time >= ? AND finish_time < ?)
            OR
            (start_time < ? AND finish_time >= ?)
          )
          {user_filter}
        """,
        job_params
    ):
        yield Job.from_tuple(row)

    for row in con.execute(
        f"""
        SELECT *
        FROM incomplete
        WHERE start_time IS NOT NULL
          AND start_time < ?
          {user_filter}
        """,
        inc_params
    ):
        yield Job.from_tuple(row)
