# Data Engineer Project Template

## 📂 Components
- `airflow_dags/` : Airflow DAGs
- `pyspark_jobs/` : PySpark scripts
- `dbt_project/`  : dbt models
- `config/`       : Configs
- `tests/`        : Unit tests

## 🚀 Setup
```bash
pip install -r requirements.txt
```

## ✅ Workflow
- Viết job PySpark trong `pyspark_jobs/`
- Orchestrate DAGs trong `airflow_dags/`
- Dùng dbt để transform dữ liệu
- Kiểm tra bằng `tests/`
