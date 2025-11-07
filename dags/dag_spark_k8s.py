from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from module.config_loader import load_env

config = load_env()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["phongtrantien294@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

SPARK_HOME = "/home/phongtt/.local/lib/python3.10/site-packages/pyspark"
SPARK_IMAGE = "registry.registry.svc.cluster.local:5000/spark:3.4.2"
K8S_MASTER = "k8s://https://192.169.203.38:6443"

# --- Spark Submit Command ---
spark_submit_cmd = f"""{SPARK_HOME}/bin/spark-submit \
--master {K8S_MASTER} \
--deploy-mode cluster \
--name IcebergLoadMetadata \
--conf spark.kubernetes.container.image={SPARK_IMAGE} \
--conf spark.kubernetes.namespace=spark \
--conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-sa \
--conf spark.driverEnv.JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 \
--conf spark.executorEnv.JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 \
--conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
--conf spark.sql.catalog.iceberg.catalog-impl=org.apache.iceberg.nessie.NessieCatalog \
--conf spark.sql.catalog.iceberg.uri={config["iceberg"]["nessie_url"]} \
--conf spark.sql.catalog.iceberg.ref=main \
--conf spark.sql.catalog.iceberg.warehouse=s3a://lake-house \
--conf spark.sql.catalog.iceberg.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
--conf spark.sql.catalog.iceberg.s3.endpoint={config["s3"]["endpoint"]} \
--conf spark.sql.catalog.iceberg.s3.access-key-id={config["s3"]["access_key"]} \
--conf spark.sql.catalog.iceberg.s3.secret-access-key={config["s3"]["secret_key"]} \
--conf spark.sql.catalog.iceberg.s3.path-style-access=true \
--conf spark.sql.catalog.iceberg.s3.region={config["s3"]["region"]} \
--conf spark.kubernetes.authenticate.submission.insecureSkipTlsVerify=true \
--conf spark.kubernetes.authenticate.driver.skipTlsVerify=true \
--files /opt/airflow/dags/ca.crt \
--conf spark.kubernetes.authenticate.submission.oauthTokenFile=/opt/airflow/dags/spark-sa.token \
--conf spark.kubernetes.file.upload.path=file:///tmp/spark-upload \
--conf spark.driver.extraJavaOptions="-Djavax.net.ssl.trustStore=/opt/spark/work-dir/ca.crt -Djavax.net.ssl.trustStorePassword=changeit" \
--conf spark.executor.extraJavaOptions="-Djavax.net.ssl.trustStore=/opt/spark/work-dir/ca.crt -Djavax.net.ssl.trustStorePassword=changeit" \
--conf spark.kubernetes.executor.volumes.hostPath.airflowdags.mount.path=/opt/airflow/dags \
--conf spark.kubernetes.executor.volumes.hostPath.airflowdags.options.path=/opt/airflow/dags \
--conf spark.kubernetes.driver.volumes.hostPath.airflowdags.mount.path=/opt/airflow/dags \
--conf spark.kubernetes.driver.volumes.hostPath.airflowdags.options.path=/opt/airflow/dags \
--conf spark.kubernetes.insecure=true \
--conf spark.executor.instances=3 \
--conf spark.executor.cores=2 \
--conf spark.executor.memory=4g \
--conf spark.driver.memory=4g \
s3a://raw/bronze/dag_spark_k8s.py
"""

with DAG(
    dag_id="dag_spark_k8s",
    default_args=default_args,
    description="Submit Spark job to Kubernetes via Airflow",
    schedule_interval='@daily',
    start_date=datetime(2025, 10, 31),
    catchup=False,
    tags=["spark", "k8s"],
) as dag:

    run_spark_job = BashOperator(
        task_id="run_spark_on_k8s",
        bash_command=spark_submit_cmd,
    )

    run_spark_job
