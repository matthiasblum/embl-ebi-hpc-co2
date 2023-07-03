import json
import re
import subprocess as sp
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any
from urllib.parse import urlencode
from urllib.request import urlopen
from urllib.error import HTTPError
from uuid import uuid4


DT_REPR = "%Y-%m-%d %H:%M:%S"


@dataclass
class Job:
    scheduler: str
    id: int
    index: int
    name: str
    status: str
    user: str
    queue: str
    slots: int
    cpu_efficiency: float
    mem_lim: int | None
    mem_max: int | None
    mem_efficiency: float
    from_host: str
    exec_host: str | None
    submit_time: datetime
    start_time: datetime | None
    finish_time: datetime | None
    cpu_time: float | None
    update_time: datetime = datetime.now()

    @property
    def accession(self) -> str:
        ts = self.submit_time.timestamp()
        return f"{ts:.0f}-{self.scheduler}-{self.id}-{self.index}"

    @property
    def ok(self) -> bool:
        if self.finish_time is not None:
            if self.scheduler == "lsf":
                if self.status.lower() == "done":
                    return True
            else:
                raise NotImplementedError(self.scheduler)

        return False

    def fix_mem(self) -> tuple[int | float | None,
                               int | float | None,
                               float | None]:
        mem_lim = self.mem_lim
        mem_eff = None
        try:
            mem_lim = 100.0 / self.mem_efficiency * self.mem_max
        except (TypeError, ZeroDivisionError):
            # May happen if mem_efficiency=0 or mem_max=None
            if self.mem_lim is not None:
                mem_eff = min(self.mem_efficiency, 100)
        else:
            mem_eff = min(self.mem_efficiency, 100)

        return mem_lim, self.mem_max, mem_eff

    def to_dict(self) -> dict:
        return asdict(self, dict_factory=self.dict_factory)

    @staticmethod
    def dict_factory(data: list[tuple[str, Any]]) -> dict[str, Any]:
        d = {}

        for attr, value in data:
            if isinstance(value, datetime):
                d[attr] = value.strftime(DT_REPR)
            else:
                d[attr] = value

        return d

    def to_tuple(self) -> tuple:
        return (
            self.accession,
            self.scheduler,
            self.id,
            self.index,
            self.name,
            self.status,
            self.user,
            self.queue,
            self.slots,
            self.cpu_efficiency,
            self.cpu_time,
            self.mem_lim,
            self.mem_max,
            self.mem_efficiency,
            self.from_host,
            self.exec_host,
            self.submit_time.strftime(DT_REPR),
            self.start_time.strftime(DT_REPR) if self.start_time else None,
            self.finish_time.strftime(DT_REPR) if self.finish_time else None,
            self.update_time.strftime(DT_REPR)
        )

    @staticmethod
    def from_tuple(obj: tuple):
        return Job(scheduler=obj[1],
                   id=obj[2],
                   index=obj[3],
                   name=obj[4],
                   status=obj[5],
                   user=obj[6],
                   queue=obj[7],
                   slots=obj[8],
                   cpu_efficiency=obj[9],
                   cpu_time=obj[10],
                   mem_lim=obj[11],
                   mem_max=obj[12],
                   mem_efficiency=obj[13],
                   from_host=obj[14],
                   exec_host=obj[15],
                   submit_time=datetime.strptime(obj[16], DT_REPR),
                   start_time=(datetime.strptime(obj[17], DT_REPR)
                               if obj[17] else None),
                   finish_time=(datetime.strptime(obj[18], DT_REPR)
                                if obj[18] else None),
                   update_time=datetime.strptime(obj[19], DT_REPR))


@dataclass
class UnixUser:
    login: str
    group: str = None
    groups: str = None

    def init(self):
        self.group, self.groups = self.get_groups(self.login)

    @staticmethod
    def get_groups(name) -> tuple:
        try:
            out = sp.check_output(["id", name],
                                  stderr=sp.DEVNULL,
                                  encoding="utf-8")
        except sp.CalledProcessError:
            return None, None

        # e.g. 100(admin)
        pattern = r"\d+\(([^\)]+)\)"

        match = re.search(r"gid=(\S+)", out).group(1)
        group = re.match(pattern, match).group(1)

        match = re.search(r"groups=(\S+)", out).group(1)
        groups = set()
        for grp in re.findall(pattern, match):
            groups.add(grp)

        return group, ",".join(sorted(groups))


@dataclass()
class User(UnixUser):
    name: str = None
    position: str = None
    teams: list[str] = field(default_factory=list)
    photo_url: str = None
    uuid: str = None
    sponsor: str = None

    def __post_init__(self):
        if self.uuid is None:
            self.uuid = uuid4().hex

    def update(self, max_attempts: int = 5):
        attempts = 0

        name = position = photo_url = teams = None

        while True:
            try:
                name, position, teams, photo_url = self.get_info(self.login)
            except HTTPError as exc:
                # TODO check for HTTP status
                attempts += 1
                if attempts < max_attempts:
                    time.sleep(0.5)
                    continue
                else:
                    raise
            else:
                break

        if name is None:
            return

        self.name = name
        if position:
            self.position = position
        if teams:
            self.teams = teams
        if photo_url:
            self.photo_url = photo_url

    @staticmethod
    def get_info(name) -> tuple[str | None, str | None, list[str], str | None]:
        params = urlencode({
            "query": name,
            "size": 100,
            "format": "JSON",
            "fields": "email,full_name,photo,positions"
        })

        url = f"https://www.ebi.ac.uk/ebisearch/ws/rest/ebiweb_people/?{params}"
        with urlopen(url) as req:
            payload = req.read().decode("utf-8")

        data = json.loads(payload)

        for entry in data["entries"]:
            obj = entry["fields"]

            email = obj["email"][0] if obj["email"] else None
            full_name = obj["full_name"][0] if obj["full_name"] else None
            teams = []
            position = None

            for e in obj["positions"]:
                if "Staff Association Representative" in e:
                    continue

                values = e.split("|")
                value = values[0].strip()
                if not position and value:
                    position = value

                value = values[1].strip()
                if value:
                    teams.append(value)

            photo_url = obj["photo"][0] if obj["photo"] else None

            if email == f"{name}@ebi.ac.uk":
                return full_name, position, teams, photo_url

        return None, None, [], None

    def to_tuple(self) -> tuple:
        return (
            self.login,
            self.name,
            self.uuid,
            json.dumps(self.teams),
            self.position,
            self.photo_url,
            self.sponsor
        )
