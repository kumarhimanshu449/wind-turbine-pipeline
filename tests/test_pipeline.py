from pathlib import Path

from wind_turbine_pipeline.config import PipelineConfig
from wind_turbine_pipeline.pipeline import run_pipeline


def test_pipeline_reads_csv_and_returns_expected_daily_grain(spark, tmp_path: Path):
    input_file = tmp_path / "data_group_test.csv"
    lines = ["timestamp,turbine_id,wind_speed,wind_direction,power_output"]
    for turbine_id in (1, 2):
        for hour in range(24):
            if not (turbine_id == 2 and hour == 7):
                lines.append(
                    f"2022-03-01 {hour:02d}:00:00,{turbine_id},12,180,{2 + turbine_id}"
                )
    input_file.write_text("\n".join(lines) + "\n", encoding="utf-8")

    result = run_pipeline(spark, str(input_file), PipelineConfig())

    assert result.cleaned.count() == 48
    assert result.cleaned.filter("is_gap_filled").count() == 1
    assert result.daily_summary.count() == 2
    assert result.anomaly_scores.filter("is_anomaly").count() == 0
    assert result.rejected.count() == 0
