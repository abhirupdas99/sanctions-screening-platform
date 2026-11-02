"""Phase 5: gold reports from the extracted Firco tables.

gold.alert_activity_monthly  - Task 2: events per month, actor type, action and decision (audit log only)
gold.user_access_review      - Task 5: one row per user-profile association, newest profile name, review flags
"""
import sys

from pyspark.sql import Window
from pyspark.sql import functions as F

from src.common import tables
from src.common.spark import get_spark
from src.jobs.ingest_day import RUN_LOG, now

JOB = "build_reports"
ADMIN, ANALYST = 30, (10, 20)          # profile_ids: System Administrator, Level 1 / Level 2 analyst


def alert_activity_monthly(spark):
    """Task 2. Reads the audit log only: the snapshot holds current status, which would rewrite past months."""
    log = tables.read(spark, "bronze", "firco_alert_audit_log")
    return (log.groupBy(F.trunc("event_timestamp", "month").alias("activity_month"),
                        "actor_type", "action_code", "decision_code")
            .agg(F.count("*").alias("event_count"),
                 F.countDistinct("alert_id").alias("distinct_alert_count"),
                 F.count("decision_code").alias("decision_event_count")))   # COUNT(column) skips NULLs


def user_access_review(spark):
    """Task 5 on the latest extract, plus the flags an access reviewer acts on."""
    def latest(name):
        df = tables.read(spark, "bronze", f"firco_{name}")
        return df.filter(F.col("extract_date") == df.agg(F.max("extract_date")).first()[0])
    users, links, names = latest("fff_users"), latest("fff_user_profile"), latest("fof_audit_profile")
    newest = Window.partitionBy("profile_id").orderBy(F.col("created_at").desc(), F.col("audit_profile_id").desc())
    profile = (names.withColumn("rn", F.row_number().over(newest))          # resolve to one row per profile FIRST
               .groupBy("profile_id")
               .agg(F.max(F.when(F.col("rn") == 1, F.col("name"))).alias("profile_name"),
                    F.count("*").alias("profile_name_versions")))
    holds = links.groupBy("user_id").agg(F.collect_set("profile_id").alias("profiles"))
    activity = (tables.read(spark, "bronze", "firco_alert_audit_log").filter("actor_type = 'USER'")
                .groupBy(F.col("actor_id").alias("username")).agg(F.max("event_timestamp").alias("last_action_at")))
    report = (users.alias("u")
              .join(links.alias("up"), F.col("u.id") == F.col("up.user_id"), "left")
              .join(profile.alias("p"), "profile_id", "left")
              .join(holds, F.col("u.id") == holds.user_id, "left")
              .join(activity, "username", "left"))
    sod = F.array_contains("profiles", ADMIN) & F.arrays_overlap("profiles", F.array(*[F.lit(a) for a in ANALYST]))
    return report.select(
        F.col("u.id").alias("user_id"), "username", "email", "active", "profile_id", "profile_name",
        "profile_name_versions",
        F.when(F.col("profile_id").isNull(), "NO_PROFILE_MAPPING")
         .when(F.col("profile_name").isNull(), "PROFILE_NOT_IN_AUDIT_TABLE").otherwise("MAPPED").alias("mapping_status"),
        F.concat_ws("; ",
                    F.when((F.col("active") == "N") & F.col("profile_id").isNotNull(), "inactive user still has access"),
                    F.when(F.coalesce(sod, F.lit(False)), "administrator also holds an analyst profile"),
                    F.when(F.col("profile_id").isNotNull() & F.col("profile_name").isNull(), "profile has no name"),
                    ).alias("review_flags"),
        "last_action_at", F.col("u.extract_date").alias("extract_date"))


def main() -> int:
    spark = get_spark(JOB)
    started = now()
    try:
        monthly = alert_activity_monthly(spark)
        tables.overwrite_partitions(monthly, "gold", "alert_activity_monthly", ["activity_month"])
        access = user_access_review(spark)
        tables.overwrite_partitions(access, "gold", "user_access_review", ["extract_date"])
        m = tables.read(spark, "gold", "alert_activity_monthly")
        a = tables.read(spark, "gold", "user_access_review")
        a = a.filter(F.col("extract_date") == a.agg(F.max("extract_date")).first()[0])
        day = a.first().extract_date
        flagged = a.filter("review_flags != ''").count()
        tables.append(spark.createDataFrame([
            ("R-" + day.strftime("%Y%m%d"), JOB, day, "alert_activity_monthly", "SUCCESS", None, m.count(), 0,
             started, now(), "Task 2 report rebuilt"),
            ("R-" + day.strftime("%Y%m%d"), JOB, day, "user_access_review", "WARNING" if flagged else "SUCCESS",
             None, a.count(), 0, started, now(), f"{flagged} rows need a reviewer's attention")], RUN_LOG),
            "ctl", "run_log")
        print("gold.alert_activity_monthly (Task 2)")
        m.orderBy("activity_month", "actor_type", "action_code", F.col("decision_code").asc_nulls_last()).show(50, False)
        print("gold.user_access_review (Task 5)")
        (a.select("user_id", "username", "active", "profile_id", "profile_name", "mapping_status", "review_flags")
         .orderBy("user_id", F.col("profile_id").asc_nulls_last()).show(50, False))
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())