from behave import fixture, use_fixture
import tempfile
from pathlib import Path


@fixture(name="fixture.tmpdir")
def tmp_dir(context):
    with tempfile.TemporaryDirectory() as tmp_dir:
        context.tmp_dir = tmp_dir
        yield tmp_dir


def before_tag(context, tag):
    if tag == "fixture.tmpdir":
        use_fixture(tmp_dir, context)


def before_all(context):
    p = Path("__SUCCESS__")
    if p.is_file():
        p.unlink()
