import tempfile
from pathlib import Path
import sys
from behave import fixture, use_fixture


@fixture(name="fixture.tmpdir")
def tmp_dir(context):
    with tempfile.TemporaryDirectory() as tmp:
        context.tmp_dir = tmp
        yield tmp


def before_tag(context, tag):
    if tag == "fixture.tmpdir":
        use_fixture(tmp_dir, context)


def before_scenario(_context, _scenario):
    p = Path("__SUCCESS__")
    if p.is_file():
        p.unlink()


def after_scenario(context, _scenario):
    if "settings" in sys.modules.keys():
        del sys.modules["settings"]
    if context.step_name in sys.modules.keys():
        del sys.modules[context.step_name]
    if context.step_name + ".step" in sys.modules.keys():
        del sys.modules[context.step_name + ".step"]
