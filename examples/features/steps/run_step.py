from behave import when, given, then
import importlib.util
from pathlib import Path

this_path = str(Path(__file__).parent.resolve())


@given("a {step_type} step exists")
def step_a_step_exists(context, step_type):
    spec = importlib.util.spec_from_file_location(
        "simplestep01.step",
        this_path + "/../../examples/simple_example01/scripts/run_step.py",
    )
    step = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(step)
    context.simple_step = step


@when("{step_type} step runs")
def step_step_runs(context, step_type):
    context.simple_step.main()


@then("it consumes and processes every message")
def step_consumes_every_message(context):
    assert Path("__SUCCESS__").is_file()


@given("step has custom lifecycle")
def step_step_has_custom_lifecycle(context):
    spec = importlib.util.spec_from_file_location(
        "simplestep01.step",
        this_path + "/../../examples/lifecycle_example01/scripts/run_step.py",
    )
    step = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(step)
    context.lifecycle_step = step


@when("{step_type} step runs with lifecycle")
def step_step_runs_lifecycle(context, step_type):
    context.lifecycle_step.main()


@then("all lifecycle methods were executed")
def step_all_lifecycle_methods_were_executed(context):
    assert context.log_capture.find_event("I'm pre_consume")
    assert context.log_capture.find_event("I'm pre_execute")
    assert context.log_capture.find_event("I'm post_execute")
    assert context.log_capture.find_event("I'm pre_produce")
    assert context.log_capture.find_event("I'm post_produce")
    assert context.log_capture.find_event("I'm tear_down")
