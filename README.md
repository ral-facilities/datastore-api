[![Build Status](https://github.com/ral-facilities/datastore-api/workflows/CI/badge.svg?branch=main)](https://github.com/ral-facilities/datastore-api/actions?query=workflow%3A%22CI%22)
[![Codecov](https://codecov.io/gh/ral-facilities/datastore-api/branch/main/graph/badge.svg)](https://codecov.io/gh/ral-facilities/datastore-api)

# Datastore-API

The Datastore API accepts requests for the archival or retrieval of experimental data.
These trigger subsequent requests to create corresponding metadata in [ICAT](https://icatproject.org/), and schedules the transfer of the data using [FTS3](https://fts3-docs.web.cern.ch/fts3-docs/).

## Development

### Environment setup
To develop the API Python development tools will need to be installed. The exact command will vary, for example on Rocky 9:

```bash
sudo yum install "@Development Tools" python3.11-devel python3.11 python3.11-setuptools openldap-devel swig gcc openssl-devel xrootd-client pipx
```

Configuration is handled via the `config.yaml` and `logging.ini` config files.

```bash
cp logging.ini.example logging.ini
cp config.yaml.example config.yaml
```

If installing on other Distros (e.g Rocky 8), pipx doesn't install as easily. So you have to install pipx seperately. [The Documentation](https://github.com/pypa/pipx?tab=readme-ov-file#on-linux) goes over the steps. You can run the @Development Tools commands normally: 

```bash
sudo yum install "@Development Tools" python3.11-devel python3.11 python3.11-setuptools openldap-devel swig gcc openssl-devel xrootd-client
```


### Poetry
[Poetry](https://python-poetry.org/) is used to manage the dependencies of this API. Note that to prevent conflicts Poetry should not be installed in the environment used for the project dependencies; [different recommended installation methods are possible](https://python-poetry.org/docs/#installing-with-the-official-installer). _(Note that pipx may not install the latest version)_

The official documentation should be referred to for the management of dependencies, but to create a Python development environment:

```bash
pipx install poetry
poetry install
```

### Nox
[Nox](https://nox.thea.codes) is used to run tests and other tools in reproducible environments. As with poetry, this is not a direct dependency of the project and so should be installed outside the poetry managed virtual environment. Nox can run sessions for the following tools:
- `black` - this uses [Black](https://black.readthedocs.io/en/stable/) to format Python code to a pre-defined style.
- `lint` - this uses [flake8](https://flake8.pycqa.org/en/latest/) with a number of additional plugins (see the included `noxfile.py` to see which plugins are used) to lint the code to keep it Pythonic. `.flake8` configures `flake8` and the plugins.
- `safety` - this uses [safety](https://github.com/pyupio/safety) to check the dependencies (pulled directly from Poetry) for any known vulnerabilities. This session gives the output in a full ASCII style report.
- `tests` - this uses [pytest](https://docs.pytest.org/en/stable/) to execute the automated tests in `tests/`. Note that this will require a running ICAT (see below).
    - `unit_tests` - as above but only runs tests in `tests/unit`, which mock dependencies on other classes and external packages.
    - `integration_tests` - as above but only runs tests in `tests/integration`, which will not mock and therefore requires services such as ICAT and FTS to be running.

To install: 
```bash
pipx install nox
```

By executing 
```bash
nox -s [SESSIONS ...]
```

### Docker
A full ICAT installation with Payara can be used by following the standard [installation tutorial](https://github.com/icatproject/icat.manual/tree/master/tutorials).
Alternatively, [Docker](https://www.docker.com/) can be used to create and manage isolated environments for the services needed to test the API. 

To install Docker for the RHEL operating system from the rpm repository, run:

```bash
sudo yum install -y yum-utils
sudo yum-config-manager --add-repo https://download.docker.com/linux/rhel/docker-ce.repo
```

This will setup the repository and install the `yum-utils` package.
To install the latest version of Docker, run:

```bash
sudo yum install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
```

_(Other installation methods can be found in the official [documentation](https://docs.docker.com/engine/install/rhel/#install-using-the-repository))._

Start Docker Daemon

```bash
sudo systemctl start docker
```

To run Docker, `cd` to the _tests_ directory containing the compose file and run:

```bash
sudo docker compose up
```

### ICAT Setup
First we need to make sure the Virtal Environment is setup correctly by running the following commands:

```bash
ls ~/.cache/pypoetry/virtualenvs/
```
After getting the name of the directory ```datastore-api-XXXXXXX_-py3.11```

```bash
source ~/.cache/pypoetry/virtualenvs/datastore-api-XXXXXXX_-py3.11/bin/activate
```

Following the above commands will create containers for ICAT and the underlying database, but not any data. The requests to the API implicitly assume that certain high level entities exist, so these should be created:

```bash
icatingest.py -i datastore_api/scripts/metadata/epac/example.yaml -f YAML --duplicate IGNORE --url http://localhost:18080 --no-check-certificate --auth simple --user root --pass pw
```

If desired the entities in `example.yaml` can be modified or extended following the [python-icat documentation](https://python-icat.readthedocs.io/en/1.3.0/icatingest.html). There are other files which will create multiple entities, if needed.

To verify that the entities are created correctly, the usual methods of inspecting the database (either by the command line within its container or via DB inspection software) or running commands against ICAT (via curl or a full stack including frontend) are possible, however for these use cases the [ICAT admin](https://icatadmin.netlify.app/) web app offers a simple and quick method of verifying the entities are created. The full address and port (e.g. http://localhost:18080 if using Docker to forward the container port to the host machine as described above) is needed along with the credentials used in the tests:
```yaml
auth: simple
username: root
password: pw
```

## Deployment
To run the API (while sourcing the virtual environment):

```bash
uvicorn --host=127.0.0.1 --port=8000 --log-config=logging.ini --reload datastore_api.main:app
```

To run from outside of the virtual environment, add `poetry run` to the beginning of the above command.
Changing the optional arguments as needed. Documentation can be found by navigating to `/docs`.
