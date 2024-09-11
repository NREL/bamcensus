import pytest
import us_census_api_py


def test_sum_as_string():
    assert us_census_api_py.sum_as_string(1, 1) == "2"
