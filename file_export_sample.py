import requests
import json
import os
import re
from urllib.parse import urlparse, unquote
from pathlib import Path

# SET THE API URL
URL = "https://api.test-datalab.nrccua.org/v1"
# SET YOUR API KEY
API_KEY = "va13sujZ9w6P96FYvP7lj1GLeC0fEsLp6NNuV1HP"
# SET YOUR ORG UID
ORGANIZATION_UID = "0ec47cfc-a5cd-4dc8-b27b-3fca9f393d8f"
# override this if you want
DOWNLOAD_DIR = Path(os.path.dirname(__file__))
# SET USERNAME
USERNAME = ""
# SET PASSWORD
PASSWORD = ""


def get_valid_filename(s):
    s = str(s).strip().replace(" ", "_")
    return re.sub(r"(?u)[^-\w.]", "", s)


# SET YOUR USERNAME AND PASSWORD
payload = {"userName": USERNAME, "password": PASSWORD, "acceptedTerms": True}

session = requests.Session()

# set the api key for the rest of the session
session.headers.update({"x-api-key": API_KEY})

# login
response_json = session.post(f"{URL}/login", data=json.dumps(payload)).json()

if "sessionToken" not in response_json:
    print(f"Couldn't find sessionToken in response json:\n {response_json}")

# set the authorization header and organization for the rest of the session
session.headers.update({"Authorization": f"JWT {response_json['sessionToken']}"})

get_exports_payload = {"status": "NotDelivered", "productKey": "score-reporter"}
response_json = session.get(
    f"{URL}/datacenter/exports", params=get_exports_payload, headers={"Organization": ORGANIZATION_UID},
).json()

# loop through results
files_to_download = []
for export in response_json:
    if "uid" in export:
        export_uid = export["uid"]
        file_export_url = f"{URL}/datacenter/exports/{export_uid}/download"
        export_response_json = session.get(file_export_url, headers={"Organization": ORGANIZATION_UID}).json()
        if "downloadUrl" in export_response_json:
            files_to_download.append(export_response_json["downloadUrl"])

if len(files_to_download) == 0:
    print(f"No files to download!")
else:
    for file in files_to_download:
        parsed_url = urlparse(file)
        # get the file name from the url, unescape it, and then replace whitespace with underscore
        escaped_filename = get_valid_filename(unquote(os.path.basename(parsed_url.path)))
        download_path = DOWNLOAD_DIR / escaped_filename
        print(f"Downloading file from url {file}")
        # don't use the session here
        download_file_response = requests.get(file, allow_redirects=True, stream=True)
        if not download_file_response.ok:
            print(f"Writing file to {download_path}.")
            with open(download_path, "wb") as f:
                # we are going to chunk the download because we don't know how large the files are
                for chunk in download_file_response.iter_content(chunk_size=1024):
                    if chunk:
                        f.write(chunk)
        else:
            print(f"There was an error retrieving {file} with status code {download_file_response.status_code}.")
            print(f"{download_file_response.content}")
