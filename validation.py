from typing import Dict, List
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum as spark_sum, when, lit

def null_check(df: DataFrame, columns: List[str]) -> Dict[str, int]:
    return {
        c: df.select(spark_sum(when(col(c).isNull(), 1).otherwise(0)).alias("nulls")).collect()[0]["nulls"]
        for c in columns
    }

def schema_check(df: DataFrame, expected_schema: Dict[str, str]) -> Dict[str, bool]:
    # expected_schema: {column_name: spark_type.simpleString()}
    actual = {f.name: f.dataType.simpleString() for f in df.schema.fields}
    return {k: (actual.get(k) == v) for k, v in expected_schema.items()}

def rowcount_check(src_count: int, tgt_count: int) -> Dict[str, int]:
    return {"source_count": src_count, "target_count": tgt_count, "diff": src_count - tgt_count}

def with_audit_columns(df: DataFrame, run_id: str) -> DataFrame:
    return df.withColumn("_run_id", lit(run_id))
