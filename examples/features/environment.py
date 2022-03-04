from behave import fixture, use_fixture
import tempfile
from pathlib import Path
import sys


@fixture(name="fixture.tmpdir")
def tmp_dir(context):
    with tempfile.TemporaryDirectory() as tmp_dir:
        context.tmp_dir = tmp_dir
        yield tmp_dir


def before_tag(context, tag):
    if tag == "fixture.tmpdir":
        use_fixture(tmp_dir, context)


def before_scenario(context, scenario):
    p = Path("__SUCCESS__")
    if p.is_file():
        p.unlink()


def after_scenario(context, scenario):
    if "settings" in sys.modules.keys():
        del sys.modules["settings"]
    if context.step_name in sys.modules.keys():
        del sys.modules[context.step_name]
    if context.step_name + ".step" in sys.modules.keys():
        del sys.modules[context.step_name + ".step"]
