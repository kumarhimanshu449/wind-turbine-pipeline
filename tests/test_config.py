import pytest

from wind_turbine_pipeline.config import PipelineConfig


@pytest.mark.parametrize(
    "kwargs",
    [
        {"max_wind_speed_mps": 0},
        {"rated_power_mw": -1},
        {"anomaly_stddev_threshold": 0},
        {"start_date": "not-a-date"},
        {"start_date": "2022-03-02", "end_date": "2022-03-01"},
    ],
)
def test_invalid_configuration_is_rejected(kwargs):
    with pytest.raises(ValueError):
        PipelineConfig(**kwargs)
