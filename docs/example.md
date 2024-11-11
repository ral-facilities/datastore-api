# Example workflow

This example intends to demonstrate the high level functionality of the Datastore API and FTS.

## Setup
Before starting, ensure:
- `logging.ini` is present in the root directory (copying from `logging.ini.example` is likely sufficient)
- `config.yaml` is present in the root directory, with:
  - `fts3.endpoint` set to a running FTS server
  - `instrument_data_cache`, `user_data_cache` and `tape_archive` set to running XRootD instances
  - `x509_user_cert` and `x509_user_key` set and the files they point to exist and are readable
- The ICAT stack is running, e.g. with Docker using `sudo docker compose -f tests/docker-compose.yaml up`
- The Datastore API is running with `poetry run uvicorn --host=127.0.0.1 --port=8000 --log-config=logging.ini --reload datastore_api.main:app`
- Source into the Virtual Env: `source ~/.cache/pypoetry/virtualenvs/datastore-api-fZJILp1_-py3.11/bin/activate`

### Create high level ICAT entities
```bash
icatingest.py -i datastore_api/scripts/metadata/epac/example.yaml -f YAML --duplicate IGNORE --url http://localhost:18080 --no-check-certificate --auth simple --user root --pass pw
```

### Create files in IDC
For simplicity, all following commands will assume that the following have been set with the correct hostnames and paths:
```bash
export INSTRUMENT_DATA_CACHE=root://hostname:1094//
export USER_DATA_CACHE=root://hostname:1094//
export TAPE_ARCHIVE=root://hostname:1094//
export TAPE_ROOT_DIR=path/to/root/dir/
```

To generate some datafiles:
```bash
fallocate -l 250M tmpfile
xrdfs $INSTRUMENT_DATA_CACHE mkdir -p /EA1/2025/ABC123-1/scan/scan_0000
xrdcp tmpfile ${INSTRUMENT_DATA_CACHE}EA1/2025/ABC123-1/scan/scan_0000/file_0000.nxs
xrdcp tmpfile ${INSTRUMENT_DATA_CACHE}EA1/2025/ABC123-1/scan/scan_0000/file_0001.nxs
xrdcp tmpfile ${INSTRUMENT_DATA_CACHE}EA1/2025/ABC123-1/scan/scan_0000/file_0002.nxs
# xrdcp tmpfile ${INSTRUMENT_DATA_CACHE}EA1/2025/ABC123-1/scan/scan_0000/file_0003.nxs
```

Note that we have commented out the final file, in order to simulate a problem with the transfer. This can be commented back in to show a fully successful transfer.

### [Optional: OpenAPI] Login to Datastore API
We have the option of using either the OpenAPI docs or command-line scripts to send requests to the API. If using the former, use the default credentials at http://127.0.0.1:8000/docs#/Login/login_login_post.
Then, copy the session id into the Authorize pop-up.

## Archival
To start, we assume we have some datafiles stored on the instrument data cache:
```bash
xrdfs $INSTRUMENT_DATA_CACHE ls /EA1/2025/ABC123-1/scan/scan_0000
```

We can also see that we have some basic, high level metadata about our facility in ICAT via the [web admin interface](https://icatadmin.netlify.app/), such as the definition of the Facility, and an Instrument. However, there are no Datafile etc. entries (yet).

We will also want to [monitor the status of any of our FTS jobs](https://fts3-test.gridpp.rl.ac.uk:8449). This can be filtered by VO, for clarity.

### Submit
The first step is to copy these files to the tape archive, via a request to the `/archive` endpoint.

#### [Optional: Command-line] Archive request
A lightweight Python script for loading metadata from file and submitting it is included with the repo:
```bash
python datastore_api/scripts/datastore.py archive datastore_api/scripts/metadata/epac/archive_request.json
```

#### [Optional: OpenAPI] Archive request
In order to transfer all 4 of our files, modify the request json to match the contents of the [file used with the Python script](../datastore_api/scripts/metadata/epac/archive_request.json):

When submitted, the API will return the id(s) of the FTS job(s) to transfer our files.

### Monitor
Straight away, we should see the metadata of our request in ICAT, including the status of our archival job as a DatasetParameter in ICAT.

For the transfer itself, we can check the status of this via the FTS monitoring UI. After a brief wait the job (containing 4 transfers) should be complete.

The API also regularly polls ICAT and FTS for the status of archival jobs, and so should update the ICAT metadata automatically with the final outcome of the job.

Finally, we can verify that the files have been transferred by looking at the storage:
```bash
xrdfs $TAPE_ARCHIVE ls /${TAPE_ROOT_DIR}EA1/2025/ABC123-1/scan/scan_0000
```

## Restore
Once the data is in the archive, it needs to be staged before transfer. FTS handles both these aspects.

### Submit
Check the ids in ICAT, as these are required to submit a restore request. In practice, a user would browse the DataGateway UI and select the relevant ICAT entities to place in their cart before requesting that data.

#### [Optional: Command-line] Restore request
The same script can be used, once the ids from ICAT are added to the [request body file](../datastore_api/scripts/metadata/epac/restore_request.json):
```bash
python datastore_api/scripts/datastore.py restore datastore_api/scripts/metadata/epac/restore_request.json
```

#### [Optional: OpenAPI] Restore request
Similarly, enter the ids into OpenAPI docs UI before executing.

### Monitor
Restore requests will not result in any changes to the ICAT metadata, but we can still monitor status in FTS. Note that due to the daemons for staging and transfers running separately, there will likely be a 5 minute delay between the staging process starting, and when the transfer of the staged file will begin. When the job is complete, the files should show under:
```bash
xrdfs $USER_DATA_CACHE ls /EA1/2025/ABC123-1/scan/scan_0000
```

## Clean up
Once completed, the following commands can be used to "reset" the files and metadata created as part of the demo.

```bash
xrdfs ${INSTRUMENT_DATA_CACHE} rm /EA1/2025/ABC123-1/scan/scan_0000/file_0000.nxs
xrdfs ${INSTRUMENT_DATA_CACHE} rm /EA1/2025/ABC123-1/scan/scan_0000/file_0001.nxs
xrdfs ${INSTRUMENT_DATA_CACHE} rm /EA1/2025/ABC123-1/scan/scan_0000/file_0002.nxs
xrdfs ${INSTRUMENT_DATA_CACHE} rm /EA1/2025/ABC123-1/scan/scan_0000/file_0003.nxs
xrdfs ${TAPE_ARCHIVE} rm /${TAPE_ROOT_DIR}EA1/2025/ABC123-1/scan/scan_0000/file_0000.nxs
xrdfs ${TAPE_ARCHIVE} rm /${TAPE_ROOT_DIR}EA1/2025/ABC123-1/scan/scan_0000/file_0001.nxs
xrdfs ${TAPE_ARCHIVE} rm /${TAPE_ROOT_DIR}EA1/2025/ABC123-1/scan/scan_0000/file_0002.nxs
xrdfs ${TAPE_ARCHIVE} rm /${TAPE_ROOT_DIR}EA1/2025/ABC123-1/scan/scan_0000/file_0003.nxs
xrdfs ${USER_DATA_CACHE} rm /EA1/2025/ABC123-1/scan/scan_0000/file_0000.nxs
xrdfs ${USER_DATA_CACHE} rm /EA1/2025/ABC123-1/scan/scan_0000/file_0001.nxs
xrdfs ${USER_DATA_CACHE} rm /EA1/2025/ABC123-1/scan/scan_0000/file_0002.nxs
xrdfs ${USER_DATA_CACHE} rm /EA1/2025/ABC123-1/scan/scan_0000/file_0003.nxs
```

Finally, delete the Dataset via the icatadmin UI (this will cascade and delete the Datafiles and Parameters, but leave the Investigation and higher level entities intact).
