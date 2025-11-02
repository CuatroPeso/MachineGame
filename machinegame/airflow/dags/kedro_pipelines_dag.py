from __future__ import annotations
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_DIR = "/opt/airflow/project"
DVC_REMOTE = "/opt/airflow/dvcstore"

default_args = {
    "owner": "you",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="kedro_dvc_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 10, 1),
    schedule_interval=None,
    catchup=False,
    tags=["kedro", "dvc"],
) as dag:

    init_dvc = BashOperator(
        task_id="init_dvc",
        bash_command=f"""
            set -ex
            cd {PROJECT_DIR}
            if [ ! -d ".dvc" ]; then
              dvc init -q
            fi
            dvc remote list | grep local_remote || dvc remote add -d local_remote {DVC_REMOTE}
            dvc remote default local_remote
            git add .dvc/config || true
            echo OK
        """,
    )

    dvc_pull = BashOperator(
        task_id="dvc_pull",
        bash_command=f"""
            set -ex
            cd {PROJECT_DIR}
            dvc pull || true
        """,
    )

    kedro_prep = BashOperator(
        task_id="kedro_data_preparation",
        bash_command=f"""
            set -ex
            cd {PROJECT_DIR}
            kedro run --pipeline data_preparation
        """,
    )

    kedro_reg = BashOperator(
        task_id="kedro_modeling_regression",
        bash_command=f"""
            set -ex
            cd {PROJECT_DIR}
            kedro run --pipeline modeling_regression
        """,
    )

    kedro_cls = BashOperator(
        task_id="kedro_modeling_classification",
        bash_command=f"""
            set -ex
            cd {PROJECT_DIR}
            kedro run --pipeline modeling_classification
        """,
    )

    dvc_push = BashOperator(
        task_id="dvc_add_and_push",
        bash_command=f"""
            set -ex
            cd {PROJECT_DIR}
            dvc add data/02_intermediate || true
            dvc add data/03_primary || true
            dvc add data/04_feature || true
            dvc add data/06_models || true
            dvc add data/07_model_output || true
            dvc add data/08_reporting || true
            git add . || true
            git commit -m "Auto: track outputs via Airflow" || true
            dvc push || true
        """,
    )

    init_dvc >> dvc_pull >> kedro_prep >> kedro_reg >> kedro_cls >> dvc_push
