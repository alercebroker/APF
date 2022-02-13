from behave import when, then
from click.testing import CliRunner
from apf.core.management.helpers import cli
from pathlib import Path


@when("user calls the new-step command")
def step_new_step(context):
    runner = CliRunner()
    with runner.isolated_filesystem(context.tmp_dir):
        runner.invoke(cli, ["new-step", "example"])


@then("a new directory is created with the step boilerplate")
def step_check_dir(context):
    p = Path(context.tmp_dir)
    dir_list = [x for x in p.iterdir() if x.is_dir()]
    step_path = Path(dir_list[0] / "example")
    dir_contents = [x for x in step_path.iterdir()]
    assert len(dir_contents) > 0


@when("user calls the new-step command with {step_type} argument")
def step_impl(context, step_type):
    runner = CliRunner()
    with runner.isolated_filesystem(context.tmp_dir):
        runner.invoke(cli, ["new-step", "example", f"--step-type={step_type}"])


@then("the config has type {step_type}")
def step_impl(context, step_type):
    raise NotImplementedError()
