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

### Create high level ICAT entities
```bash
icatingest.py -i datastore_api/scripts/example.yaml -f YAML --duplicate IGNORE --url http://localhost:18080 --no-check-certificate --auth simple --user root --pass pw
```

### Create files in IDC
For simplicity, all following commands will assume that the following have been set with the correct hostnames and paths:
```bash
INSTRUMENT_DATA_CACHE=root://hostname:1094//
USER_DATA_CACHE=root://hostname:1094//
TAPE_ARCHIVE=root://hostname:1094//
TAPE_ROOT_DIR=path/to/root/dir/
```

To generate some datafiles:
```bash
fallocate -l 250M tmpfile
xrdfs $INSTRUMENT_DATA_CACHE mkdir -p /instrument/20XX/ABC123-1/scan/scan_0000
xrdcp tmpfile ${INSTRUMENT_DATA_CACHE}instrument/20XX/ABC123-1/scan/scan_0000/file_0000.nxs
xrdcp tmpfile ${INSTRUMENT_DATA_CACHE}instrument/20XX/ABC123-1/scan/scan_0000/file_0001.nxs
xrdcp tmpfile ${INSTRUMENT_DATA_CACHE}instrument/20XX/ABC123-1/scan/scan_0000/file_0002.nxs
xrdcp tmpfile ${INSTRUMENT_DATA_CACHE}instrument/20XX/ABC123-1/scan/scan_0000/file_0003.nxs
```

### Login to Datastore API
Use the default credentials at http://127.0.0.1:8000/docs#/Login/login_login_post.
Then, copy the session id into the Authorize pop-up.

## Archival
To start, we assume we have some datafiles stored on the instrument data cache:
```bash
xrdfs $INSTRUMENT_DATA_CACHE ls /instrument/20XX/ABC123-1/scan/scan_0000
```

We can also see that we have some basic, high level metadata about our facility in ICAT via the [web admin interface](https://vigorous-lamarr-7b3487.netlify.app/), such as the definition of the Facility, and an Instrument. However, there are no Datafile etc. entries (yet).

We will also want to [monitor the status of any of our FTS jobs](https://fts3-test.gridpp.rl.ac.uk:8449). This can be filtered by VO, for clarity.

### Submit
The first step is to copy these files to the tape archive, via a request to the `/archive` endpoint. In order to transfer all 4 of our files, modify the request json to:
```json
{
  "investigations": [
    {
      "facility": {
        "name": "facility"
      },
      "investigationType": {
        "name": "type"
      },
      "instrument": {
        "name": "instrument"
      },
      "facilityCycle": {
        "name": "20XX"
      },
      "datasets": [
        {
          "name": "scan_0000",
          "complete": true,
          "description": "Description",
          "doi": "10.00000/00000",
          "location": "string",
          "startDate": "2024-06-05T10:53:29.501Z",
          "endDate": "2024-06-05T10:53:29.501Z",
          "datasetType": {
            "name": "scan"
          },
          "datafiles": [
            {
              "name": "file_0000.nxs",
              "description": "Description",
              "doi": "10.00000/00000",
              "fileSize": 0,
              "checksum": "string",
              "datafileCreateTime": "2024-06-05T10:53:29.501Z",
              "datafileModTime": "2024-06-05T10:53:29.501Z"
            },
            {
              "name": "file_0001.nxs",
              "description": "Description",
              "doi": "10.00000/00000",
              "fileSize": 0,
              "checksum": "string",
              "datafileCreateTime": "2024-06-05T10:53:29.501Z",
              "datafileModTime": "2024-06-05T10:53:29.501Z"
            },
            {
              "name": "file_0002.nxs",
              "description": "Description",
              "doi": "10.00000/00000",
              "fileSize": 0,
              "checksum": "string",
              "datafileCreateTime": "2024-06-05T10:53:29.501Z",
              "datafileModTime": "2024-06-05T10:53:29.501Z"
            },
            {
              "name": "file_0003.nxs",
              "description": "Description",
              "doi": "10.00000/00000",
              "fileSize": 0,
              "checksum": "string",
              "datafileCreateTime": "2024-06-05T10:53:29.501Z",
              "datafileModTime": "2024-06-05T10:53:29.501Z"
            }
          ]
        }
      ],
      "name": "ABC123",
      "visitId": "1",
      "title": "Title",
      "summary": "Summary",
      "doi": "10.00000/00000",
      "startDate": "2024-06-05T10:53:29.501Z",
      "endDate": "2024-06-05T10:53:29.501Z",
      "releaseDate": "2024-06-05T10:53:29.501Z"
    }
  ]
}
```

When submitted, the API will return the id(s) of the FTS job(s) to transfer our files.

### Monitor
Straight away, we should see the metadata of our request in ICAT, including the status of our archival job as a DatasetParameter in ICAT.

For the transfer itself, we can check the status of this via the FTS monitoring UI. After a brief wait the job (containing 4 transfers) should be complete.

The API also regularly polls ICAT and FTS for the status of archival jobs, and so should update the ICAT metadata automatically with the final outcome of the job.

Finally, we can verify that the files have been transferred by looking at the storage:
```bash
xrdfs $TAPE_ARCHIVE ls /${TAPE_ROOT_DIR}instrument/20XX/ABC123-1/scan/scan_0000
```

## Restore
Once the data is in the archive, it needs to be staged before transfer. FTS handles both these aspects.

### Submit
Check the ids in ICAT, as these are required to submit a restore request. In practice, a user would browse the DataGateway UI and select the relevant ICAT entities to place in their cart before requesting that data. 

### Monitor
Restore requests will not result in any changes to the ICAT metadata, but we can still monitor status in FTS. Note that due to the daemons for staging and transfers running separately, there will likely be a 5 minute delay between the staging process starting, and when the transfer of the staged file will begin. When the job is complete, the files should show under:
```bash
xrdfs $USER_DATA_CACHE ls /instrument/20XX/ABC123-1/scan/scan_0000
```
