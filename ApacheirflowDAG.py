from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime

# Mock Claim Data
claim_data = {
    "is_insured": True,
    "is_covered": True,
    "severity": 7  # Scale 1-10
}

# Decision Functions
def check_insurance(**kwargs):
    return "reject_claim" if not claim_data["is_insured"] else "check_coverage"

def check_coverage(**kwargs):
    return "reject_claim" if not claim_data["is_covered"] else "assess_severity"

def assess_severity(**kwargs):
    return "approve_claim" if claim_data["severity"] <= 5 else "manual_review"

def approve_claim():
    print("Claim Approved!")

def reject_claim():
    print("Claim Denied!")

def manual_review():
    print("Claim requires manual review.")

# DAG Definition
default_args = {"owner": "airflow", "start_date": datetime(2024, 2, 18), "catchup": False}

dag = DAG(
    "automobile_claims_processing",
    default_args=default_args,
    schedule_interval=None
)

start = DummyOperator(task_id="start", dag=dag)

check_insurance_task = BranchPythonOperator(
    task_id="check_insurance",
    python_callable=check_insurance,
    provide_context=True,
    dag=dag
)

check_coverage_task = BranchPythonOperator(
    task_id="check_coverage",
    python_callable=check_coverage,
    provide_context=True,
    dag=dag
)

assess_severity_task = BranchPythonOperator(
    task_id="assess_severity",
    python_callable=assess_severity,
    provide_context=True,
    dag=dag
)

approve_claim_task = PythonOperator(task_id="approve_claim", python_callable=approve_claim, dag=dag)
reject_claim_task = PythonOperator(task_id="reject_claim", python_callable=reject_claim, dag=dag)
manual_review_task = PythonOperator(task_id="manual_review", python_callable=manual_review, dag=dag)

end = DummyOperator(task_id="end", dag=dag)

# Task Dependencies
start >> check_insurance_task
check_insurance_task >> [reject_claim_task, check_coverage_task]
check_coverage_task >> [reject_claim_task, assess_severity_task]
assess_severity_task >> [approve_claim_task, manual_review_task]
[approve_claim_task, reject_claim_task, manual_review_task] >> end
