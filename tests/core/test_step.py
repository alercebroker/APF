from apf.core.step import GenericStep
from apf.producers import GenericProducer
import pytest


@pytest.fixture
def basic_config():
    return {
        "CONSUMER_CONFIG": {
            "PARAMS": {},
            "CLASS": "apf.consumers.generic.GenericConsumer",
        },
    }


def test_get_single_extra_metrics():
    config = {
        "METRICS_CONFIG": {
            "CLASS": "apf.metrics.GenericMetricsProducer",
            "PARAMS": {},
            "EXTRA_METRICS": ["oid", "candid"],
        },
        "CONSUMER_CONFIG": {
            "PARAMS": {},
            "CLASS": "apf.consumers.generic.GenericConsumer",
        },
    }
    message = {"oid": "TEST", "candid": 1}
    gs = GenericStep(config=config)
    gs.message = message
    gs._pre_execute()
    gs._post_execute(None)
    extra_metrics = gs.metrics
    assert type(extra_metrics) is dict
    assert type(extra_metrics["oid"]) is str
    assert type(extra_metrics["candid"]) is int
    assert type(extra_metrics["n_messages"]) is int


def test_get_batch_extra_metrics():
    config = {
        "METRICS_CONFIG": {
            "CLASS": "apf.metrics.GenericMetricsProducer",
            "PARAMS": {},
            "EXTRA_METRICS": [
                "oid",
                "candid",
                {
                    "key": "candid",
                    "alias": "str_candid",
                    "format": lambda x: str(x),
                },
            ],
        },
        "CONSUMER_CONFIG": {
            "PARAMS": {},
            "CLASS": "apf.consumers.generic.GenericConsumer",
        },
    }
    message = [{"oid": "TEST", "candid": 1}, {"oid": "TEST2"}, {"candid": 3}]
    gs = GenericStep(config=config)
    gs.message = message
    gs._pre_execute()
    gs._post_execute(None)
    extra_metrics = gs.metrics
    assert type(extra_metrics) is dict
    assert type(extra_metrics["oid"]) is list
    assert type(extra_metrics["candid"]) is list
    assert type(extra_metrics["n_messages"]) is int


def test_get_value():
    config = {
        "METRICS_CONFIG": {
            "CLASS": "apf.metrics.GenericMetricsProducer",
            "PARAMS": {},
            "EXTRA_METRICS": ["oid", "candid"],
        },
        "CONSUMER_CONFIG": {
            "PARAMS": {},
            "CLASS": "apf.consumers.generic.GenericConsumer",
        },
    }
    message = {"oid": "TEST", "candid": 1}
    gs = GenericStep(config=config)

    aliased_metric, value = gs.get_value(message, "oid")
    assert aliased_metric == "oid"
    assert value == "TEST"

    aliased_metric, value = gs.get_value(message, "candid")
    assert aliased_metric == "candid"
    assert value == 1

    aliased_metric, value = gs.get_value(message, {"key": "oid"})
    assert aliased_metric == "oid"
    assert value == "TEST"

    aliased_metric, value = gs.get_value(
        message, {"key": "oid", "alias": "new_oid"}
    )
    assert aliased_metric == "new_oid"
    assert value == "TEST"

    aliased_metric, value = gs.get_value(
        message, {"key": "oid", "format": lambda x: x[0]}
    )
    assert aliased_metric == "oid"
    assert value == "T"

    with pytest.raises(KeyError):
        gs.get_value(message, {})

    with pytest.raises(ValueError):
        gs.get_value(message, {"key": "oid", "format": "test"})

    with pytest.raises(ValueError):
        gs.get_value(message, {"key": "oid", "alias": 1})


def test_step_type(basic_config):
    basic_config.update({"STEP_TYPE": "abcdefg"})
    with pytest.raises(Exception):
        GenericStep(basic_config)
    basic_config.update({"STEP_TYPE": "composite"})
    gs = GenericStep(basic_config)
    assert gs.step_type == "composite"
    basic_config.update({"STEP_TYPE": "component"})
    gs = GenericStep(basic_config)
    assert gs.step_type == "component"
    basic_config.update({"STEP_TYPE": "simple"})
    gs = GenericStep(basic_config)
    assert gs.step_type == "simple"


def test_without_consumer_config(basic_config):
    basic_config.update({"CONSUMER_CONFIG": {}})
    with pytest.raises(Exception):
        GenericStep(basic_config)


def test_with_producer_config(basic_config):
    basic_config.update(
        {"PRODUCER_CONFIG": {"CLASS": "apf.producers.generic.GenericProducer"}}
    )
    gs = GenericStep(basic_config)
    assert isinstance(gs.producer, GenericProducer)


def test_send_metrics(basic_config):
    basic_config.update(
        {
            "METRICS_CONFIG": {
                "CLASS": "apf.metrics.GenericMetricsProducer",
                "PARAMS": {},
            },
        }
    )
    gs = GenericStep(basic_config)
    gs.send_metrics(metric1="a", metric2="b")


def test_start(basic_config):
    gs = GenericStep(basic_config)
    gs.start()
