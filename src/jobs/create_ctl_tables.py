"""Phase 1: create the two control tables as Delta. Safe to run again (IF NOT EXISTS)."""
from src.common.config import LAKEHOUSE
from src.common.spark import get_spark

DDL = {
    "file_manifest": """
        file_name        STRING    NOT NULL,
        source_system    STRING    NOT NULL,
        business_date    DATE      NOT NULL,
        received_at      TIMESTAMP NOT NULL,
        checksum_algo    STRING,
        checksum_result  STRING    NOT NULL,
        row_count        BIGINT,
        status           STRING    NOT NULL,
        run_id           STRING    NOT NULL
    """,
    "run_log": """
        run_id           STRING    NOT NULL,
        job_name         STRING    NOT NULL,
        business_date    DATE      NOT NULL,
        stage            STRING    NOT NULL,
        status           STRING    NOT NULL,
        rows_in          BIGINT,
        rows_out         BIGINT,
        rows_rejected    BIGINT,
        started_at       TIMESTAMP NOT NULL,
        ended_at         TIMESTAMP,
        message          STRING
    """,
}


def main() -> None:
    spark = get_spark("create-ctl-tables")
    for name, columns in DDL.items():
        path = (LAKEHOUSE / "ctl" / name).as_posix()
        spark.sql(f"CREATE TABLE IF NOT EXISTS delta.`{path}` ({columns}) USING DELTA")
        count = spark.read.format("delta").load(path).count()
        print(f"ctl.{name}: ready at {path} ({count} rows)")
    spark.stop()


if __name__ == "__main__":
    main()