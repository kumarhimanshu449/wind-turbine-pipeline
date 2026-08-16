"""Configuration values shared by pipeline transformations."""

from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class PipelineConfig:
    """Business rules for the proof-of-concept pipeline.

    The physical limits are deliberately configurable. They should come from a
    turbine registry in a production system because different turbine models
    can have different rated capacities and operating envelopes.
    """

    max_wind_speed_mps: float = 60.0
    rated_power_mw: float = 5.0
    anomaly_stddev_threshold: float = 2.0
    start_date: str | None = None
    end_date: str | None = None

    def __post_init__(self) -> None:
        if self.max_wind_speed_mps <= 0:
            raise ValueError("max_wind_speed_mps must be greater than zero")
        if self.rated_power_mw <= 0:
            raise ValueError("rated_power_mw must be greater than zero")
        if self.anomaly_stddev_threshold <= 0:
            raise ValueError("anomaly_stddev_threshold must be greater than zero")
        start = date.fromisoformat(self.start_date) if self.start_date else None
        end = date.fromisoformat(self.end_date) if self.end_date else None
        if start and end and start > end:
            raise ValueError("start_date must not be later than end_date")
