# -*- coding: utf-8 -*-
# configuration of pytest
# inspiré de https://docs.pytest.org/en/latest/example/simple.html#control-skipping-of-tests-according-to-command-line-option
import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--on-spark", action="store_true", default=False, help="run with spark tests"
    )


def pytest_collection_modifyitems(config, items):
    if config.getoption("--on-spark"):
        # --on-spark given in cli: do not skip spark tests
        return
    skip_spark = pytest.mark.skip(reason="need --on-spark option to run")
    for item in items:
        if "spark" in item.keywords:
            item.add_marker(skip_spark)
