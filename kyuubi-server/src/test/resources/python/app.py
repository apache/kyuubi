from module1.module import func1

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType

if __name__ == "__main__":
    print(f"Started running PySpark app at {func1()}")

    spark = SparkSession.builder.appName("pyspark-sample").getOrCreate()
    sc = spark.sparkContext

    data = [1, 2, 3, 4, 5]
    rdd = sc.parallelize(data)
    transformed_rdd = rdd.map(lambda x: x * 2)
    collected = transformed_rdd.collect()

    df = spark.createDataFrame(transformed_rdd, IntegerType())
    df.coalesce(1).write.format("csv").option("header", "false").save("/tmp/" + func1())

    print(f"Result: {collected}")
