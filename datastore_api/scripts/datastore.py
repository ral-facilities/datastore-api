import argparse
import json

import requests


def archive():
    parser = argparse.ArgumentParser(
        prog="Datastore",
        description="Submit request to the Datastore API",
    )
    parser.add_argument("request", choices={"archive", "restore"})
    parser.add_argument("file")
    parser.add_argument("-u", "--url", default="http://localhost:8000")
    parser.add_argument("-a", "--auth", default="simple")
    parser.add_argument("-n", "--username", default="root")
    parser.add_argument("-p", "--password", default="pw")
    args = parser.parse_args()

    credentials = {"username": args.username, "password": args.password}
    login_json = {"auth": args.auth, "credentials": credentials}
    login_response = requests.post(url=f"{args.url}/login", json=login_json)
    session_id = json.loads(login_response.content.decode())["sessionId"]
    headers = {"Authorization": f"Bearer {session_id}"}

    with open(args.file) as f:
        request_json = json.load(f)

    if args.request == "archive":
        restore_response = requests.post(
            url=f"{args.url}/archive",
            headers=headers,
            json=request_json,
        )
        if restore_response.status_code != 200:
            raise requests.HTTPError(restore_response.content)
        response_dict = json.loads(restore_response.content.decode())
        print("Archive submitted with response:\n", response_dict)

    elif args.request == "restore":
        restore_response = requests.post(
            url=f"{args.url}/restore/rdc",
            headers=headers,
            json=request_json,
        )
        if restore_response.status_code != 200:
            raise requests.HTTPError(restore_response.content)
        response_dict = json.loads(restore_response.content.decode())
        print("Restore submitted with response:\n", response_dict)


if __name__ == "__main__":
    archive()
