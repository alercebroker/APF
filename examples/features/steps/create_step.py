from behave import when, then
from click.testing import CliRunner
from apf.core.management.helpers import cli
from pathlib import Path
import importlib.util


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
def step_call_new_step(context, step_type):
    runner = CliRunner()
    with runner.isolated_filesystem(context.tmp_dir):
        runner.invoke(cli, ["new-step", f"--step-type={step_type}", "example"])


@then("the config has type {step_type}")
def step_check_step_type(context, step_type):
    p = Path(context.tmp_dir)
    dir_list = [x for x in p.iterdir() if x.is_dir()]
    step_path = Path(dir_list[0] / "example")
    spec = importlib.util.spec_from_file_location(
        str(step_path),
        step_path / "settings.py",
    )
    settings = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(settings)
    assert settings.STEP_CONFIG.get("STEP_TYPE") == step_type


@then("the step is not able to run")
def step_step_not_able_to_run(context):
    p = Path(context.tmp_dir)
    dir_list = [x for x in p.iterdir() if x.is_dir()]
    step_path = Path(dir_list[0] / "example")
    spec = importlib.util.spec_from_file_location(
        "step",
        step_path / "scripts/run_step.py",
    )
    step = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(step)
    try:
        step = step.Example(config=step.STEP_CONFIG)
    except Exception as e:
        assert "Step type can only be one of" in str(e)
