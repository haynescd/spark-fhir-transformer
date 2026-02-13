import glob

from pyspark.sql import SparkSession


def main():

    RESOURCE_TYPE = "Patient"
    # 1. Initialize the session and connect to your Docker Master
    spark = SparkSession.builder.appName("MyFirstClusterJob").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    files = glob.glob(f"/opt/spark/data/{RESOURCE_TYPE}/*.ndjson")

    # 2. Create some dummy data
    df = spark.read.option("multiLine", "false").json(files)

    print(f"Patients loaded {df.count()}")

    # 4. Show the result
    # df.show()

    # 5. Stop the session
    spark.stop()


if __name__ == "__main__":
    main()
