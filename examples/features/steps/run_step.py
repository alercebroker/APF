from behave import when, given, then
import importlib.util
from pathlib import Path

this_path = str(Path(__file__).parent.resolve())


@given("{step_name} step")
def step_a_step_exists(context, step_name):
    spec = importlib.util.spec_from_file_location(
        step_name,
        this_path + f"/../../examples/{step_name}/scripts/run_step.py",
    )
    step = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(step)
    context.step = step


@when("step runs")
def step_step_runs(context):
    context.step.main()


@then("it consumes and processes every message")
def step_consumes_every_message(context):
    assert Path("__SUCCESS__").is_file()


@then("all lifecycle methods were executed")
def step_all_lifecycle_methods_were_executed(context):
    assert context.log_capture.find_event("I'm pre_consume")
    assert context.log_capture.find_event("I'm pre_execute")
    assert context.log_capture.find_event("I'm post_execute")
    assert context.log_capture.find_event("I'm pre_produce")
    assert context.log_capture.find_event("I'm post_produce")
    assert context.log_capture.find_event("I'm tear_down")
