# New Mac VsCode setup
## Open the VS Code terminal
cd /wind-turbine-pipeline
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

## Run code-quality validation

Run Ruff:

ruff check .

Expected result:

All checks passed!

## Run the automated tests

Run:

pytest -v

This will:

Start a local Spark session.
Test configuration validation.
Test duplicate removal.
Test malformed data.
Test physical outlier handling.
Test missing-hour generation.
Test hierarchical imputation.
Test circular wind-direction averaging.
Test daily statistics.
Test anomaly detection.
Test database table storage.
Run an end-to-end CSV test.

You should see approximately:

11 passed

## Run the complete monthly pipeline
From the repository root, with .venv active, run:

python -m wind_turbine_pipeline --input "data/raw/*.csv"

This will perform below steps:

Start Spark.
Read the three CSVs.
Parse and validate the data.
Create the hourly grid.
Calculate imputation statistics.
Calculate daily summaries.
Calculate fleet anomaly scores.
Initialise the local Spark database.
Write four tables.

Spark will create generated content under:

build/

The main table data will be located under:

build/spark-warehouse/

The local metastore will be located under:

build/spark-metastore/

These directories are excluded by .gitignore.

# Setup Git
## Initialize Git
1. git init -b main

2. Verify the repository
git status

It should now show:

On branch main
No commits yet

Your project files will appear under Untracked files.

3. Stage the files
git add .

Check what will be committed:

git status

Make sure .venv, __pycache__, metastore_db and derby.log are not listed.

4. Create the first commit
git commit -m "Build PySpark wind turbine data pipeline"

If Git asks you to configure your identity, run:

git config --global user.name "<github username>"
git config --global user.email "YOUR_GITHUB_EMAIL"

Then repeat:

git commit -m "Build PySpark wind turbine data pipeline"

After the commit succeeds, create an empty repository named wind-turbine-pipeline on GitHub. Do not add a GitHub-generated README, .gitignore, or licence. Then connect and upload it:

git remote add origin https://github.com/YOUR_USERNAME/wind-turbine-pipeline.git
git push -u origin main

Replace YOUR_USERNAME with your real GitHub username. Your active (.venv) can remain active throughout these steps.