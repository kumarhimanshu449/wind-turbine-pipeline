# Wind Turbine Data Pipeline

A small, testable PySpark proof-of-concept that ingests hourly wind-turbine
telemetry, repairs data-quality issues, calculates 24-hour power statistics,
detects fleet-level anomalies, and stores the results as queryable Spark SQL
tables.

## Quick start

Prerequisites: Python 3.10+, Java 17, and a Unix-like shell. Python 3.11 is used
in CI.

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"

pytest
python -m wind_turbine_pipeline --input "data/raw/*.csv"
```

The default run creates these Parquet-backed tables in the `wind_energy` Spark
database under `build/spark-warehouse`:

- `cleaned_measurements`
- `turbine_daily_summary`
- `turbine_daily_anomalies`
- `rejected_records`

The command is also installed as `wind-turbine-pipeline`. For example, process
one daily batch with custom turbine limits:

```bash
wind-turbine-pipeline \
  --input "data/raw/*.csv" \
  --start-date 2022-03-15 \
  --end-date 2022-03-15 \
  --rated-power-mw 5.0 \
  --max-wind-speed-mps 60.0 \
  --anomaly-threshold 2.0
```

## Design

```text
CSV files
  -> explicit string schema and safe parsing
  -> reject invalid business keys
  -> deterministic de-duplication
  -> physical-range validation
  -> hourly gap completion and hierarchical imputation
  -> turbine/day summary statistics
  -> fleet/day anomaly scoring
  -> Spark SQL database tables
```

The code separates orchestration, transformations, configuration, schemas, and
storage. The transformation entry point has no write side effects, which keeps
the core logic easy to test and allows a different storage adapter to be added
without rewriting the data rules.

### Data cleaning

| Issue | Treatment |
| --- | --- |
| Invalid timestamp or turbine ID | Quarantine in `rejected_records` |
| Duplicate `(timestamp, turbine_id)` | Keep the most complete row; use a stable fingerprint as a tie-breaker |
| Missing hourly sensor row | Generate the expected turbine/date/hour key and mark `is_gap_filled` |
| Missing wind speed or power | Turbine-day median, then turbine median, then fleet median |
| Missing wind direction | Turbine-day, turbine, then fleet circular mean |
| Physically impossible reading | Convert to null, mark its outlier flag, then apply the same imputation rule |

Default physical ranges are:

- wind speed: 0 to 60 m/s
- wind direction: 0 inclusive to 360 exclusive degrees
- power output: 0 to the configured 5 MW rated capacity

These limits are configuration, not hidden constants. A production pipeline
would look them up by turbine model from a reference-data table.

Plausible but unusual values are deliberately retained. Removing them during
cleaning would hide the operational signal that anomaly detection is intended
to find.

### Summary statistics

Each UTC calendar day is one 24-hour window. For each turbine and day, the
pipeline records the required minimum, maximum, and average MW output. It also
stores sample standard deviation, observed/expected counts, imputation counts,
cleaned-outlier counts, and a completeness ratio to make the result auditable.

### Anomaly definition

The phrase “turbines whose output is outside two standard deviations from the
mean” is interpreted at the same grain as the requested 24-hour summary:

1. Calculate each turbine's average output for a UTC day.
2. Calculate the fleet mean and sample standard deviation of those turbine
   averages for that day.
3. Flag a turbine when its daily average is strictly outside
   `fleet mean +/- 2 * fleet standard deviation`.

The scored summary retains the z-score and upper/lower bounds. The anomaly
table contains only flagged turbine-days. When fleet standard deviation is zero
or unavailable, no turbine is flagged because a meaningful z-score cannot be
calculated.

## Supplied-data profile

The three provided files contain a complete, valid March 2022 dataset:

| Measure | Result |
| --- | ---: |
| Input rows | 11,160 |
| Turbines | 15 |
| Hourly readings per turbine | 744 |
| Duplicate turbine/timestamp keys | 0 |
| Missing values | 0 |
| Missing hourly slots | 0 |
| Daily summary rows | 465 |

Because the supplied data has no quality defects, unit tests inject missing
hours, nulls, duplicate keys, malformed values, and impossible readings. The
source CSVs are not changed.

Using the stated fleet/day rule, the supplied data produces 17 anomaly rows.
This does not by itself prove equipment failure; an alert would normally also
require persistence over several windows and context such as wind conditions
and maintenance state.

## Tests

```bash
pytest --cov=wind_turbine_pipeline --cov-report=term-missing
ruff check .
```

The test suite covers:

- configuration validation;
- safe parsing, rejection, deterministic de-duplication, and outlier handling;
- creation and imputation of a missing hourly record;
- circular averaging at the 0/360-degree boundary;
- required daily summary statistics;
- positive anomaly detection and the zero-anomaly case; and
- an end-to-end CSV integration test.

GitHub Actions runs linting and all tests on pushes and pull requests.

## Assumptions and trade-offs

- Input timestamps are hourly and interpreted in UTC.
- A selected date represents a complete 24-hour batch. The grid therefore
  creates all 24 expected hours for every observed turbine.
- Each file contains a stable group of turbines, as stated in the assignment.
- Rated power is 5 MW for the sample. It is configurable because the source
  data does not supply a turbine model or rated capacity.
- The POC recomputes and overwrites its local analytical tables, making repeat
  runs deterministic for the growing monthly CSVs. It does not pretend that
  local Hive/Parquet storage is a production transactional sink.
- Median/circular-mean imputation is appropriate for this exercise. Imputed
  flags and completeness measures are retained so downstream consumers can
  exclude imputed values if required.

## Productionisation discussion

For production I would retain these transformations but change the execution
and operational envelope:

- land immutable files in object storage and track file name, checksum, size,
  and ingestion time in a control table;
- use streaming file discovery or an orchestrator and process only new files;
- write Delta Lake or Iceberg tables and use a merge keyed by turbine and
  timestamp for idempotency and late-arriving corrections;
- obtain turbine-specific capacity and operating limits from governed reference
  data rather than CLI defaults;
- quarantine malformed records with structured reason codes and alert on data
  quality/completeness thresholds;
- partition by date, compact small files, and tune shuffle partitions based on
  observed volume;
- add schema-contract checks, lineage, metrics, retries, and dashboards;
- test anomaly logic against a historical baseline segmented by turbine model,
  season, wind speed, and maintenance state before turning flags into alerts;
- deploy with infrastructure as code and separate dev/test/prod catalogs,
  service identities, secrets, and retention policies.

## Repository layout

```text
.
|-- .github/workflows/ci.yml
|-- data/raw/                         # supplied assignment CSVs
|-- src/wind_turbine_pipeline/
|   |-- config.py                     # thresholds and date range
|   |-- schemas.py                    # explicit CSV schema
|   |-- transformations.py            # DataFrame business logic
|   |-- pipeline.py                   # read/transform orchestration
|   |-- storage.py                    # Spark SQL table adapter
|   `-- main.py                       # CLI and Spark session
|-- tests/
|-- Makefile
`-- pyproject.toml
```


# New Mac VsCode setup
## Open the VS Code terminal
cd /Users/himanshukumar/development/wind-turbine-pipeline
pwd
ls

## Create the Python virtual environment
python3.11 -m venv .venv
source .venv/bin/activate

## Select the virtual environment in VS Code
In VS Code:

Press Cmd + Shift + P.
Search for Python: Select Interpreter.
Select the interpreter under:
./.venv/bin/python

If it does not appear:

Select Enter interpreter path.
Select Find.
Navigate to:
wind-turbine-pipeline/.venv/bin/python

VS Code normally discovers workspace-local .venv environments automatically. VS Code Python environment documentation.

After selecting it, open a new terminal. It should automatically activate .venv.

## Upgrade the Python installation tools
With .venv active, run:

python -m pip install --upgrade pip setuptools wheel

Verify:

python -m pip --version

The displayed path should include .venv.

## Install java
(.venv) @Mac wind-turbine-pipeline % brew --version
(.venv) @Mac wind-turbine-pipeline % brew install --cask temurin@17

## Check if JAVA_HOME is configured properly
1. Open a new VS Code terminal:
Terminal → New Terminal
2. Before activating .venv, run:
/usr/libexec/java_home -V
3. If Java 17 is installed, configure it:
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
export PATH="$JAVA_HOME/bin:$PATH"
4. Verify:
echo "$JAVA_HOME"
java -version
5. Then activate the Python environment:
source .venv/bin/activate
6. Verify both Java:
java -version

## Install the project and its dependencies
Run:

python -m pip install -e ".[dev]"

The quotation marks are important because Zsh may interpret square brackets itself.

This command installs:

PySpark
pytest
pytest-cov
Ruff
The wind_turbine_pipeline package
The wind-turbine-pipeline command

PySpark is a large download, so this step may take several minutes. Installing it with pip includes the Spark components required for local execution, so you do not need to install a separate Spark distribution. This is supported by the official PySpark installation guide.

Verify PySpark:

python -c "import pyspark; print(pyspark.__version__)"

Verify the project package:

python -c "import wind_turbine_pipeline; print(wind_turbine_pipeline.__file__)"

The second command should display a path under this repository’s src directory.

## Verify the complete environment
Run each command:

python --version
java -version
echo "$JAVA_HOME"
python -c "import pyspark; print('PySpark:', pyspark.__version__)"
pytest --version
ruff --version