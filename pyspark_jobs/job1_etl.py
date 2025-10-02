from pyspark_jobs.utils.spark_helper import get_spark

def etl_job():
    spark = get_spark("Job1")
    df = spark.range(10).toDF("num")
    df.show()

if __name__ == "__main__":
    etl_job()
