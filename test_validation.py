from pyspark.sql import SparkSession
from src.validation import null_check, schema_check, rowcount_check

def test_null_check():
    spark = SparkSession.builder.appName("test").getOrCreate()
    df = spark.createDataFrame(
        [(1, None), (2, "x"), (3, None)],
        ["id", "val"]
    )
    res = null_check(df, ["val"])
    assert res["val"] == 2
    spark.stop()

def test_schema_check():
    spark = SparkSession.builder.appName("test").getOrCreate()
    df = spark.createDataFrame([(1, "a")], ["id", "name"])
    expected = {"id": "bigint", "name": "string"}
    res = schema_check(df, expected)
    assert all(res.values())
    spark.stop()

def test_rowcount_check():
    res = rowcount_check(100, 90)
    assert res["diff"] == 10
