import nox
from pathlib import Path
home = Path.home()
nox.options.envdir = home / ".cache/.nox"

@nox.session(python=["3.10", "3.11", "3.12"])
def test(session):
    session.install(".")
    session.install("pytest")
    session.run("pytest", "tests/")
