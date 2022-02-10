from behave import fixture, use_fixture
import tempfile


@fixture
def tmp_dir(context):
    with tempfile.TemporaryDirectory() as tmp_dir:
        context.tmp_dir = tmp_dir
        yield tmp_dir


def before_all(context):
    use_fixture(tmp_dir, context)
