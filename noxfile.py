import os
import tempfile

import nox

# Separating Black away from the rest of the sessions
nox.options.sessions = "lint", "safety", "tests"
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
        )

        try:
            # Due to delete=False, the file must be deleted manually
            requirements.close()
            os.unlink(requirements.name)
        except IOError:
            session.log("Error: The temporary requirements file could not be closed")


@nox.session(python=["3.11"], reuse_venv=True)
def unit_tests(session):
    args = session.posargs or ["--cov", "--cov-report=term-missing"]
    session.run("poetry", "install", "--with=dev", external=True)
    session.run("pytest", "tests/unit", *args)


@nox.session(python=["3.11"], reuse_venv=True)
def integration_tests(session):
    args = session.posargs or ["--cov", "--cov-report=term-missing"]
    session.run("poetry", "install", "--with=dev", external=True)
    session.run("pytest", "tests/integration", "tests", *args)


@nox.session(python=["3.11"], reuse_venv=True)
def tests(session):
    args = session.posargs or ["--cov", "--cov-report=term-missing"]
    session.run("poetry", "install", "--with=dev", external=True)
    session.run("pytest", "tests", *args)
