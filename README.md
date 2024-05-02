[![Build Status](https://github.com/ral-facilities/datastore-api/workflows/CI/badge.svg?branch=main)](https://github.com/ral-facilities/datastore-api/actions?query=workflow%3A%22CI%22)
[![Codecov](https://codecov.io/gh/ral-facilities/datastore-api/branch/main/graph/badge.svg)](https://codecov.io/gh/ral-facilities/datastore-api)

# Datastore-API

The Datastore API accepts requests for the archival or retrieval of experimental data.
These trigger subsequent requests to create corresponding metadata in [ICAT](https://icatproject.org/), and schedules the transfer of the data using [FTS3](https://fts3-docs.web.cern.ch/fts3-docs/).

## Deployment
To run the API:

```bash
uvicorn --host=127.0.0.1 --port=8000 --log-config=logging.ini --reload datastore_api.main:app
```

Changing the optional arguments as needed. Documentation can be found by navigating to `/docs`.

## Development

### Environment setup
To develop the API Python development tools will need to be installed. The exact command will vary, for example on Rocky 8:

```bash
sudo yum install "@Development Tools" python3.11-devel python3.11 python3.11-setuptools openldap-devel swig gcc openssl-devel
```

### Poetry
[Poetry](https://python-poetry.org/) is used to manage the dependencies of this API. Note that to prevent conflicts Poetry should not be installed in the environment used for the project dependencies; [different recommended installation methods are possible](https://python-poetry.org/docs/#installing-with-pipx).

The official documentation should be referred to for the management of dependencies, but to create a Python development environment:

```bash
poetry install
```

### Nox
[Nox](https://nox.thea.codes) is used to run tests and other tools in reproducible environments. As with poetry, this is not a direct dependency of the project and so should be installed outside the poetry managed virtual environment. Nox can run sessions for the following tools:
- `black` - this uses [Black](https://black.readthedocs.io/en/stable/) to format Python code to a pre-defined style.
- `lint` - this uses [flake8](https://flake8.pycqa.org/en/latest/) with a number of additional plugins (see the included `noxfile.py` to see which plugins are used) to lint the code to keep it Pythonic. `.flake8` configures `flake8` and the plugins.
- `safety` - this uses [safety](https://github.com/pyupio/safety) to check the dependencies (pulled directly from Poetry) for any known vulnerabilities. This session gives the output in a full ASCII style report.
- `tests` - this uses [pytest](https://docs.pytest.org/en/stable/) to execute the automated tests in `tests/`.
    - `unit_tests` - as above but only runs tests in `tests/unit`, which mock dependencies on other classes and external packages.
    - `integration_tests` - as above but only runs tests in `tests/integration`, which will not mock and therefore requires services such as ICAT and FTS to be running.

By executing 
```bash
nox -s [SESSIONS ...]
```

### ICAT Setup
A full ICAT installation with Payara can be used by following the standard [installation tutorial](https://github.com/icatproject/icat.manual/tree/master/tutorials). Alternatively, the containers used for tests can provide a minimal setup which should be sufficient for development and manual testing:

```bash
sudo docker compose -f tests/docker-compose.yaml up
```

This will create containers for ICAT and the underlying database, but not any data. The requests to the API implicitly assume that certain high level entities exist, so these should be created:

```bash
icatingest.py -i datastore_api/scripts/example.yaml -f YAML --duplicate IGNORE --url http://localhost:18080 --no-check-certificate --auth simple --user root --pass pw
```

If desired the entities in `example.yaml` can be modified or extended following the [python-icat documentation](https://python-icat.readthedocs.io/en/1.3.0/icatingest.html). There are other files which will create multiple entities, if needed.
