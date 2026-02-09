from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

DATA_PATH = "/Users/madankumar/Documents/SEM 2/Data_Engineering_2/Exam/cybersecurity_logs_dataset.csv"

def Load_Data():
    df = pd.read_csv(DATA_PATH)
    df.to_csv("/tmp/raw.csv", index=False)

def Clean_Data():
    df = pd.read_csv("/tmp/raw.csv")
    df = df.drop_duplicates()
    df["location"] = df["location"].fillna("Unknown")
    df.to_csv("/tmp/clean.csv", index=False)

def Aggregate_Data():
    df = pd.read_csv("/tmp/clean.csv")
    agg = df.groupby("user_id")["request_count"].sum()
    agg.to_csv("/tmp/agg.csv")

def Feature_Engineering():
    df = pd.read_csv("/tmp/clean.csv")
    df["risk_score"] = df["request_count"] * (df["login_status"]=="fail")
    df.to_csv("/tmp/features.csv", index=False)

with DAG(
    dag_id="cybersecurity_pipeline",
    start_date=datetime(2024,1,1),
    schedule="@daily",
    catchup=False
) as dag:

    t1 = PythonOperator(task_id="Load_Data", python_callable=Load_Data)
    t2 = PythonOperator(task_id="Clean_Data", python_callable=Clean_Data)
    t3 = PythonOperator(task_id="Aggregate_Data", python_callable=Aggregate_Data)
    t4 = PythonOperator(task_id="Feature_Engineering", python_callable=Feature_Engineering)

    t1 >> t2 >> t3 >> t4


