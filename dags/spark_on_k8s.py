from airflow import DAG
from datetime import datetime

try:
    from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
except ModuleNotFoundError:
    try:
        from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
    except ModuleNotFoundError:
        from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

with DAG(
    dag_id="iceberg_load_metadata_direct_py",
    start_date=datetime(2025, 11, 4),
    schedule=None,
    catchup=False,
    tags=["spark", "k8s", "iceberg"],
) as dag:

    spark_submit = KubernetesPodOperator(
        task_id="spark_submit_k8s_py",
        name="spark-submit-iceberg-py",
        namespace="spark",
        kubernetes_conn_id="kubernetes_default",   # dùng Connection thay vì env host/port
        service_account_name="spark-sa",
        image="{{ var.value.SPARK_IMAGE | default('10.96.236.105:5000/spark:3.4.2-v0.1') }}",
        cmds=["/opt/spark/bin/spark-submit"],
        arguments=[
            "--master", "{{ var.value.K8S_MASTER }}",
            "--deploy-mode", "cluster",
            "--name", "IcebergLoadMetadata",
            "--conf", "spark.kubernetes.container.image=10.96.236.105:5000/spark:3.4.2-v0.1",
            "--conf", "spark.kubernetes.namespace=spark",
            "--conf", "spark.kubernetes.authenticate.driver.serviceAccountName=spark-sa",
            "--conf", "spark.kubernetes.authenticate.submission.insecureSkipTlsVerify=true",
            "--conf", "spark.kubernetes.authenticate.driver.skipTlsVerify=true",
            "--conf", "spark.kubernetes.authenticate.submission.oauthTokenFile=/opt/airflow/dags/spark-ca.token",
            "--conf", "spark.driverEnv.JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64",
            "--conf", "spark.executorEnv.JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64",
            "--conf", "spark.kubernetes.driverEnv.HOME=/opt/spark",
            "--conf", "spark.executorEnv.HOME=/opt/spark",
            "--conf", "spark.jars.ivy=/opt/spark/.ivy2",
            "--conf", "spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog",
            "--conf", "spark.sql.catalog.iceberg.catalog-impl=org.apache.iceberg.nessie.NessieCatalog",
            "--conf", "spark.sql.catalog.iceberg.uri={{ var.json.config.iceberg.nessie_url }}",
            "--conf", "spark.sql.catalog.iceberg.ref=main",
            "--conf", "spark.sql.catalog.iceberg.warehouse=s3a://lake-house",
            "--conf", "spark.sql.catalog.iceberg.io-impl=org.apache.iceberg.aws.s3.S3FileIO",
            "--conf", "spark.sql.catalog.iceberg.s3.endpoint={{ var.json.config.s3.endpoint }}",
            "--conf", "spark.sql.catalog.iceberg.s3.access-key-id={{ var.json.config.s3.access_key }}",
            "--conf", "spark.sql.catalog.iceberg.s3.secret-access-key={{ var.json.config.s3.secret_key }}",
            "--conf", "spark.sql.catalog.iceberg.s3.path-style-access=true",
            "--conf", "spark.sql.catalog.iceberg.s3.region={{ var.json.config.s3.region }}",
            "--conf", "spark.kubernetes.file.upload.path=file:///tmp/spark-upload",
            "--conf", "spark.kubernetes.executor.volumes.hostPath.airflowdags.mount.path=/opt/airflow/dags",
            "--conf", "spark.kubernetes.executor.volumes.hostPath.airflowdags.options.path=/opt/airflow/dags",
            "--conf", "spark.kubernetes.driver.volumes.hostPath.airflowdags.mount.path=/opt/airflow/dags",
            "--conf", "spark.kubernetes.driver.volumes.hostPath.airflowdags.options.path=/opt/airflow/dags",
            "--conf", 'spark.driver.extraJavaOptions=-Djavax.net.ssl.trustStore=/opt/airflow/dags/ca.crt -Djavax.net.ssl.trustStorePassword=changeit',
            "--conf", 'spark.executor.extraJavaOptions=-Djavax.net.ssl.trustStore=/opt/airflow/dags/ca.crt -Djavax.net.ssl.trustStorePassword=changeit',
            "--archives", "/opt/airflow/dags/venv.tar.gz#venv",
            "--conf", "spark.pyspark.python=venv/bin/python",
            "--conf", "spark.pyspark.driver.python=venv/bin/python",
            "--conf", "spark.kubernetes.insecure=true",
            "--conf", "spark.executor.instances=3",
            "--conf", "spark.executor.cores=2",
            "--conf", "spark.executor.memory=4g",
            "--conf", "spark.driver.memory=4g",
            "local:///opt/airflow/dags/job1_etl.py",
        ],
        get_logs=True,
        is_delete_operator_pod=True,
    )