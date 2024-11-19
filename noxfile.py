import os
import tempfile

from codecarbon import OfflineEmissionsTracker
import nox

# Separating Black away from the rest of the sessions
nox.options.sessions = "lint", "safety", "unit_tests", "integration_tests"
code_locations = "datastore_api", "tests", "noxfile.py"


@nox.session(reuse_venv=True)
def black(session):
    args = session.posargs or code_locations
    session.run("poetry", "install", "--only=dev", external=True)
    session.run("black", *args, external=True)


@nox.session(reuse_venv=True)
def lint(session):
    args = session.posargs or code_locations
    session.run("poetry", "install", "--only=dev", external=True)
    session.run("flake8", *args)


@nox.session(reuse_venv=True)
def safety(session):
    session.run("poetry", "install", "--only=dev", external=True)
    with tempfile.NamedTemporaryFile(delete=False) as requirements:
        session.run(
            "poetry",
            "export",
            "--with=dev",
            "--format=requirements.txt",
            "--without-hashes",
            f"--output={requirements.name}",
            external=True,
        )
        session.run(
            "safety",
            "check",
            f"--file={requirements.name}",
            "--full-report",
            "--ignore=70612",
        )

        try:
            # Due to delete=False, the file must be deleted manually
            requirements.close()
            os.unlink(requirements.name)
        except IOError:
            session.log("Error: The temporary requirements file could not be closed")


@nox.session(python=["3.11"], reuse_venv=True)
def tests(session):
    """Runs all tests."""
    with OfflineEmissionsTracker(
        country_iso_code="GBR",
        save_to_file=False,
        measure_power_secs=60 * 60,
    ):
        args = session.posargs or ["--cov", "--cov-report=term-missing"]
        session.run("poetry", "install", "--with=dev", external=True)
        session.run("pytest", "tests", *args)


@nox.session(python=["3.11"], reuse_venv=True)
def unit_tests(session):
    """Runs only the tests which target individual functions. These may typically mock
    responses from other components to make tests specific and quick.
    """
    with OfflineEmissionsTracker(
        country_iso_code="GBR",
        save_to_file=False,
        measure_power_secs=60 * 60,
    ):
        args = session.posargs or ["--cov", "--cov-report=term-missing"]
        session.run("poetry", "install", "--with=dev", external=True)
        session.run("pytest", "tests/unit", *args)


@nox.session(python=["3.11"], reuse_venv=True)
def integration_tests(session):
    """Runs only the tests which target the endpoints in main.py. These should only mock
    responses from other components when absolutely necessary (i.e. from FTS when certs
    are not present) to make the tests as realistic as practicable.
    """
    with OfflineEmissionsTracker(
        country_iso_code="GBR",
        save_to_file=False,
        measure_power_secs=60 * 60,
    ):
        args = session.posargs or ["--cov", "--cov-report=term-missing"]
        session.run("poetry", "install", "--with=dev", external=True)
        session.run("pytest", "tests/integration", *args)
