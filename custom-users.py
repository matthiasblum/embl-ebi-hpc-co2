import json
from argparse import ArgumentParser

from ebihpc import usagedb


def main():
    parser = ArgumentParser(description="Update users metadata")
    parser.add_argument("database", help="usage database")
    parser.add_argument("users", help="users JSON file")
    parser.add_argument("login", help="login of user to update")
    args = parser.parse_args()

    db_file = args.database
    js_file = args.users
    login = args.login

    con = usagedb.connect(db_file)
    db_users = {u.login: u for u in usagedb.get_users(con, {})}
    con.close()

    try:
        fh = open(js_file)
    except FileNotFoundError:
        js_users = {}
    else:
        js_users = json.load(fh)
        fh.close()

    if login in db_users:
        user = db_users[login]
        _print("Database", user.name, user.position, user.teams, user.sponsor,
               user.photo_url)

    if login in js_users:
        user = js_users[login]
        _print("JSON", user["name"], user["position"], user["teams"],
               user["sponsor"], user.get("photo_url"))
    else:
        user = {}

    print("Enter new metadata.")
    print("If a field is left empty, the value will not be updated,")
    print("unless the user is not already in the JSON file,")
    print("in which case it will be set to None.")
    print("Enter 'N/A' (case insensitive) to force a field to be set to None.")
    print("Multiple teams can be specified with a pipe (|) separator.")

    name = parse_str(input(f"{'Name':<15}"), user.get("name"))
    position = parse_str(input(f"{'Position':<15}"), user.get("position"))
    teams = parse_list(input(f"{'Teams':<15}"), user.get("teams", []))
    sponsor = parse_str(input(f"{'Sponsor':<15}"), user.get("sponsor"))
    photo_url = parse_str(input(f"{'Photo':<15}"), user.get("photo_url"))

    print("")
    _print("New metadata", name, position, teams, sponsor, photo_url)

    if input("Proceed (y/[n])? ").strip().lower() == "y":
        js_users[login] = {
            "name": name,
            "position": position,
            "teams": teams,
            "sponsor": sponsor,
            "photo_url": photo_url
        }
        with open(js_file, "wt" )as fh:
            json.dump(js_users, fh, indent=4)

        print("Updated")
    else:
        print("Aborted")


def parse_str(s: str, default: str | None) -> str | None:
    s = s.strip()
    if s.lower() == "n/a":
        return None
    return s or default


def parse_list(s: str, default: list | None) -> list[str]:
    s = s.strip()
    if s.lower() == "n/a":
        return []

    lst = {e.strip() for e in s.split("|") if e.strip()}
    return sorted(lst) or default


def _print(title: str, name: str | None, position: str | None, teams: list[str],
           sponsor: str | None, photo_url: str | None):
    print(f"{title}\n{'-' * len(title)}")
    print(f"{'Name':<15}{name or 'N/A'}")
    print(f"{'Position':<15}{position or 'N/A'}")
    if teams:
        print(f"{'Teams':<15}{', '.join(sorted(teams))}")
    else:
        print(f"{'Teams':<15}N/A")

    print(f"{'Sponsor':<15}{sponsor or 'N/A'}")
    print(f"{'Photo':<15}{photo_url or 'N/A'}")
    print("")


if __name__ == '__main__':
    main()
