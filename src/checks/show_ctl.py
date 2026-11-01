"""Print the control tables: what arrived, what the gate decided, what each run did."""
from pyspark.sql import functions as F

from src.common import tables
from src.common.spark import get_spark

spark = get_spark("show-ctl")
print("ctl.file_manifest: files that are not plainly ACCEPTED")
(tables.read(spark, "ctl", "file_manifest")
 .filter("status != 'ACCEPTED' OR row_count = 0")
 .select("business_date", "file_name", "checksum_algo", "checksum_result", "row_count", "status")
 .orderBy("business_date").show(truncate=False))
print("ctl.file_manifest: totals by status")
tables.read(spark, "ctl", "file_manifest").groupBy("status").count().orderBy("status").show()
print("ctl.run_log: stages that were not SUCCESS")
(tables.read(spark, "ctl", "run_log").filter(F.col("status") != "SUCCESS")
 .select("business_date", "stage", "status", "rows_in", "rows_out", "rows_rejected", "message")
 .orderBy("business_date", "stage").show(truncate=60))
spark.stop()