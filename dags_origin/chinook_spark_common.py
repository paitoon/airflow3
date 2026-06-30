from pathlib import Path

# [FIX] ใช้ local JARs แทน --packages เพื่อหลีกเลี่ยง network download จาก Maven Central
# JARs อยู่ใน ./jars/ ที่ mount เป็น /opt/airflow/jars ใน container
_JARS_DIR = "/opt/airflow/jars"
_JARS = [
    "delta-spark_2.12-3.3.2.jar",
    "delta-storage-3.3.2.jar",
    "hadoop-aws-3.3.4.jar",
    "aws-java-sdk-bundle-1.12.262.jar",
    "antlr4-runtime-4.9.3.jar",
    "wildfly-openssl-1.0.7.Final.jar",
]

# SPARK_PACKAGES = None เพื่อปิด --packages ใน SparkSubmitOperator
SPARK_PACKAGES = None

SPARK_CONF = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
    "spark.hadoop.fs.s3a.access.key": "minio",
    "spark.hadoop.fs.s3a.secret.key": "minio123",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    # [FIX] ชี้ไปที่ local JARs แทนการ download
    "spark.jars": ",".join(f"{_JARS_DIR}/{jar}" for jar in _JARS),
    # [FIX] cross-user write: driver (UID 50000) สร้าง _temporary/0/ → executor (UID 185) ต้องเขียนข้างในได้
    # umask=000 ทำให้ directory ที่ driver สร้างเป็น 777 แทน 755
    "spark.hadoop.fs.permissions.umask-mode": "000",
    "spark.eventLog.enabled": "true",
    "spark.eventLog.dir": "file:///opt/spark/logs/spark-events",
}
