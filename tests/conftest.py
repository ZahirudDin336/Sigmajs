"""Test setup: an isolated Airflow home with a throwaway SQLite metadata DB."""
import os
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
_AIRFLOW_HOME = tempfile.mkdtemp(prefix="airflow_home_")

# Must be set before airflow is imported anywhere.
os.environ["AIRFLOW_HOME"] = _AIRFLOW_HOME
os.environ["AIRFLOW__CORE__DAGS_FOLDER"] = str(ROOT / "dags")
os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "True"
os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = f"sqlite:///{_AIRFLOW_HOME}/airflow.db"
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
# CDEJobRunOperator resolves its connection when the DAG is parsed. In CDE the
# connection is pre-provisioned; here a local "Virtual cluster" connection is used.
os.environ["AIRFLOW_CONN_CDE_RUNTIME_API"] = (
    '{"conn_type": "cloudera_data_engineering", "host": "https://cde-local.example.com/dex/api/v1"}'
)

for path in (ROOT / "dags", ROOT / "spark"):
    sys.path.insert(0, str(path))

import pytest  # noqa: E402  (env must be set before airflow is imported)


@pytest.fixture(scope="session")
def airflow_db():
    from airflow.utils import db

    db.resetdb()
    yield
