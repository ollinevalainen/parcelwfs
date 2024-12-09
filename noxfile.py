import nox
from pathlib import Path

home = Path.home()
nox.options.envdir = home / ".cache/.nox"


@nox.session(python=["3.10", "3.11", "3.12"])
def test(session):
    if session.python == "3.12":
        session.run("python", "-m", "ensurepip", "--upgrade")
        session.run("python", "-m", "pip", "install", "--upgrade", "setuptools")
    session.install(".")
    session.install("pytest")
    session.run("pytest", "tests/")
