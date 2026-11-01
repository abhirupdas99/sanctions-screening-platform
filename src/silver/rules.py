"""Phase 4 silver rules as DataFrame transformations: Task 1 (validate, dedupe) and Task 4 (change detection).

Each function takes a DataFrame and returns a new one, so the rules can be read and tested on their own.
"""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

COMPARED = ["name", "country_code", "position_held", "company_name", "date_of_birth"]   # Task 4 "change" columns


def blank_to_null(col):
    """'' and '   ' mean 'no value': store them as NULL so comparisons treat them the same."""
    return F.when(F.trim(col) == "", None).otherwise(col)


def validate(df: DataFrame) -> DataFrame:
    """Task 1 validation + matching keys. Every row is kept; bad rows get every reason, not just the first."""
    sid = blank_to_null(F.trim("record_id"))
    numeric = sid.rlike("^[0-9]+$")
    out = (df
           .withColumn("sid_num", F.when(numeric, sid.cast("long")))       # convert only what is safe
           .withColumn("name_key", blank_to_null(F.upper(F.regexp_replace(F.trim("name"), r"\s+", " "))))
           .withColumn("cc_key", blank_to_null(F.upper(F.trim("country_code"))))
           .withColumn("validation_error", F.concat_ws("; ",                # concat_ws skips NULLs
               F.when(sid.isNull(), F.lit("source_record_id missing")),
               F.when(sid.isNotNull() & ~numeric, F.lit("source_record_id not numeric")),
               F.when(F.col("name_key").isNull(), F.lit("name missing")),
               F.when(F.col("cc_key").isNull(), F.lit("country_code missing"))))
           .withColumn("validation_error", blank_to_null(F.col("validation_error"))))
    for c in COMPARED:                                                     # blanks become NULL for Task 4
        out = out.withColumn(c, blank_to_null(F.col(c)))
    return out


def dedupe(df: DataFrame) -> DataFrame:
    """Task 1 parent assignment: same person = same name_key + cc_key, across BOTH source systems.
    Parent = lowest numeric ID, then source, then file line (deterministic). Invalid rows never join a group."""
    group = Window.partitionBy("name_key", "cc_key", F.col("validation_error").isNull()).orderBy(
        "sid_num", "source_system", "load_seq")
    ranked = (df
              .withColumn("rn", F.row_number().over(group))
              .withColumn("parent_source", F.first("source_system").over(group))
              .withColumn("parent_record_id", F.first("record_id").over(group)))
    return (ranked
            .withColumn("status", F.when(F.col("validation_error").isNotNull(), "INVALID")
                        .when(F.col("rn") == 1, "NOT_DUPLICATE").otherwise("DUPLICATE"))
            .withColumn("duplicate_of", F.when(F.col("status") == "DUPLICATE",
                                               F.concat_ws(":", "parent_source", "parent_record_id")))
            .withColumn("group_key", F.when(F.col("status") != "INVALID", F.concat_ws("|", "name_key", "cc_key")))
            .drop("rn", "parent_source", "parent_record_id"))


def detect_changes(today: DataFrame, previous: DataFrame | None) -> DataFrame:
    """Task 4 at record level: compare each valid record with the previous business date, NULL-safe.
    NEW (not there before), DELETED (gone today), UPDATES (any compared column differs), NO_UPDATES.

    Record IDs are not guaranteed unique (Task 1 showed the same ID used by different people). If an ID
    appears more than once on either day, we can't tell which old row became which new one, so those rows
    are sent for screening again (NEW, reason 'ambiguous_record_id'): over-screening is the safe direction."""
    keys = ["source_system", "record_id"]
    one_per_key = Window.partitionBy(*keys)
    t = (today.filter("status != 'INVALID'")
         .select(*keys, *[F.col(c).alias(f"t_{c}") for c in COMPARED])
         .withColumn("t_n", F.count("*").over(one_per_key)))
    if previous is None:                                                   # first day: everything is new
        return t.select(*keys, F.lit("NEW").alias("change_type"),
                        F.array().cast("array<string>").alias("changed_columns"))
    p = (previous.filter("status != 'INVALID'")
         .select(*keys, *[F.col(c).alias(f"p_{c}") for c in COMPARED])
         .withColumn("p_n", F.count("*").over(one_per_key)))
    t_keys = t.groupBy(*keys).agg(F.max("t_n").alias("t_n"))
    p_keys = p.groupBy(*keys).agg(F.max("p_n").alias("p_n"))
    k = t_keys.join(p_keys, keys, "full_outer").withColumn(
        "ambiguous", (F.coalesce("t_n", F.lit(1)) > 1) | (F.coalesce("p_n", F.lit(1)) > 1))

    # 1. today's rows whose ID is ambiguous: re-screen
    amb = (t.join(k.filter("ambiguous").select(*keys), keys)
           .select(*keys, F.lit("NEW").alias("change_type"),
                   F.array(F.lit("ambiguous_record_id")).alias("changed_columns")))
    # 2. previous rows whose ID is gone today (ambiguous or not): DELETED, one row each
    gone = (p.join(t_keys.select(*keys), keys, "left_anti")
            .select(*keys, F.lit("DELETED").alias("change_type"),
                    F.array().cast("array<string>").alias("changed_columns")))
    # 3. unique IDs: the normal Task 4 comparison
    unique = k.filter(~F.col("ambiguous")).select(*keys)
    j = (t.join(unique, keys).drop("t_n").withColumn("_t", F.lit(True))
         .join(p.join(unique, keys).drop("p_n").withColumn("_p", F.lit(True)), keys, "left"))
    diff = F.array_compact(F.array(*[F.when(~F.col(f"t_{c}").eqNullSafe(F.col(f"p_{c}")), F.lit(c))
                                     for c in COMPARED]))                  # <=> : NULL equals NULL
    normal = j.select(
        *keys,
        F.when(F.col("_p").isNull(), "NEW").when(F.size(diff) > 0, "UPDATES").otherwise("NO_UPDATES")
         .alias("change_type"),
        F.when(F.col("_p").isNull(), F.array().cast("array<string>")).otherwise(diff).alias("changed_columns"))
    return normal.unionByName(amb).unionByName(gone)