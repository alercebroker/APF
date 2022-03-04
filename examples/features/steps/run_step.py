from pathlib import Path
import importlib.util
from behave import when, given, then
import pandas as pd

THIS_PATH = str(Path(__file__).parent.resolve())


@given("{step_name} step")
def step_a_step_exists(context, step_name):
    spec = importlib.util.spec_from_file_location(
        step_name,
        THIS_PATH + f"/../../examples/{step_name}/scripts/run_step.py",
    )
    step = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(step)
    context.step = step
    context.step_name = step_name


@given("{data} directory has no output")
def step_data_has_no_output(_context, data):
    data_path = THIS_PATH + f"/../../examples/{data}"
    path = Path(data_path)
    to_delete = [
        path / "inner_data1.csv",
        path / "inner_data2.csv",
        path / "inner_input.csv",
        path / "output.csv",
    ]
    for to_del in to_delete:
        if to_del.exists():
            to_del.unlink()


@then("{data} directory has no output")
def step_then_data_has_no_output(context, data):
    step_data_has_no_output(context, data)


@when("step runs")
def step_step_runs(context):
    context.step.main()


@then("it consumes and processes every message")
def step_consumes_every_message(_context):
    assert Path("__SUCCESS__").is_file()


@then("all lifecycle methods were executed")
def step_all_lifecycle_methods_were_executed(context):
    assert context.log_capture.find_event("I'm pre_consume")
    assert context.log_capture.find_event("I'm pre_execute")
    assert context.log_capture.find_event("I'm post_execute")
    assert context.log_capture.find_event("I'm pre_produce")
    assert context.log_capture.find_event("I'm post_produce")
    assert context.log_capture.find_event("I'm tear_down")


@then("it produces joined output in {directory}")
def step_join_messages(_context, directory):
    df = pd.read_csv(THIS_PATH + f"/../../examples/{directory}/output.csv")
    assert df["data_in1"].tolist() == [1, 3]
    assert df["data_in2"].tolist() == [2, 4]
