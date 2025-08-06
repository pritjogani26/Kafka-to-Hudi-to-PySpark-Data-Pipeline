# snapshot_service/read_hudi_snapshot.py

from pyspark.sql import SparkSession
from py4j.protocol import Py4JJavaError
import time


def get_spark_session():
    return SparkSession.builder \
        .appName("HudiSnapshotQuery") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
        .config("spark.jars", "/opt/extra_jars/hudi-spark3.4-bundle_2.12-0.14.1.jar") \
        .getOrCreate()


def read_using_filter(spark, base_path, country, state, city):
    print(f"\n[Method 1] Reading full table and filtering in Spark: {country}/{state}/{city}")
    try:
        start = time.time()

        df = spark.read.format("hudi") \
            .option("hoodie.datasource.query.type", "snapshot") \
            .load(base_path)

        filtered_df = df.filter(
            f"country = '{country}' AND state = '{state}' AND city = '{city}'"
        )

        count = filtered_df.count()
        print(f"Filtered records: {count}")
        filtered_df.select("id", "firstname", "state", "city", "country").show(20, truncate=False)

        end = time.time()
        print(f"Method 1 Execution Time: {end - start:.2f} seconds")

    except Exception as e:
        print("Error during full table filter read:")
        print(e)


def read_using_partition_path(spark, base_path, country, state, city):
    print(f"\n[Method 2] Reading only specific partition path: {country}/{state}/{city}")
    try:
        start = time.time()

        partition_path = f"{base_path}/{country}/{state}/{city}"
        df = spark.read.format("hudi").load(partition_path)

        count = df.count()
        print(f"Records in partition: {count}")
        df.select("id", "firstname", "state", "city", "country").show(20, truncate=False)

        end = time.time()
        print(f"Method 2 Execution Time: {end - start:.2f} seconds")

    except Py4JJavaError as e:
        print("Error during partition path read.")
        if "NullPointerException" in str(e):
            print("Likely no committed data in the specified partition.")
        print("Details:\n", e)

    except Exception as e:
        print("General error:")
        print(e)


if __name__ == "__main__":
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    hudi_base_path = "/hudi_output"
    country, state, city = "India", "Gujarat", "Rajkot"

    read_using_filter(spark, hudi_base_path, country, state, city)
    read_using_partition_path(spark, hudi_base_path, country, state, city)

    spark.stop()
