from pathlib import Path
import importlib.util
from behave import when, then
from click.testing import CliRunner
from apf.core.management.helpers import cli
from apf.core.step import SimpleStep, ComponentStep, CompositeStep


@when("user calls the new-step command")
def step_new_step(context):
    runner = CliRunner()
    with runner.isolated_filesystem(context.tmp_dir):
        result = runner.invoke(cli, ["new-step", "example"])
        context.runner_result = result


@then("a new directory is created with the step boilerplate")
def step_check_dir(context):
    p = Path(context.tmp_dir)
    dir_list = [x for x in p.iterdir() if x.is_dir()]
    step_path = Path(dir_list[0] / "example")
    dir_contents = [x for x in step_path.iterdir()]
    assert len(dir_contents) > 0
    assert context.runner_result.exit_code == 0


@when("user calls the new-step command with {step_type} argument")
def step_call_new_step(context, step_type):
    runner = CliRunner()
    with runner.isolated_filesystem(context.tmp_dir):
        result = runner.invoke(
            cli, ["new-step", f"--step-type={step_type}", "example"]
        )
        context.runner_result = result


@then("the created step has type {step_type}")
def step_check_step_type(context, step_type):
    p = Path(context.tmp_dir)
    dir_list = [x for x in p.iterdir() if x.is_dir()]
    step_path = Path(dir_list[0] / "example")
    spec = importlib.util.spec_from_file_location(
        "example",
        step_path / "example/step.py",
    )
    step = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(step)
    step_map = {
        "composite": CompositeStep,
        "component": ComponentStep,
        "simple": SimpleStep,
    }
    assert issubclass(
        step.Example,
        step_map[step_type],
    )


@then("step can't be created")
def step_step_not_created(context):
    assert context.runner_result.exit_code != 0
