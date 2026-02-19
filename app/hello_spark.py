import glob
import re

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import DataType, StringType, StructField, StructType
from radiant_fhir_transform_cli.transform.classes import (
    PatientTransformer,
    transformers,
)
from radiant_fhir_transform_cli.transform.classes.base import FhirResourceTransformer


def sanitize_for_fhir(obj):
    """Recursively converts NumPy types to native Python types."""
    if isinstance(obj, dict):
        return {k: sanitize_for_fhir(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [sanitize_for_fhir(i) for i in obj]
    elif isinstance(obj, np.ndarray):
        return sanitize_for_fhir(obj.tolist())
    elif isinstance(obj, (np.integer, np.int64, np.int32)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float64, np.float32)):
        return float(obj)
    elif isinstance(obj, (np.bool_, bool)):
        return bool(obj)
    else:
        return obj


def camel_to_snake(name: str) -> str:
    # Add underscore before uppercase letters (excluding first char)
    s1 = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)

    # Handle multiple uppercase sequences (e.g. JSONParser -> json_parser)
    return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


def generate_table_name(resource_type: str, resource_subtype: str | None) -> str:
    table_name = camel_to_snake(resource_type)

    if resource_subtype:
        table_name = f"{table_name}_{camel_to_snake(resource_subtype)}"

    return table_name


def transform_resources(transformer: FhirResourceTransformer):
    def transform(df_iter):
        for df in df_iter:
            # Convert DataFrame to list of dictionaries (resources)
            resources = df.to_dict("records")

            clean = sanitize_for_fhir(resources)
            # Transform resources using the FHIR transformer
            transformed_resources = transformer.transform_resources(clean)

            pdf = pd.DataFrame(transformed_resources)
            pdf = pdf.astype("string")

            # Convert back to DataFrame for Spark
            if transformed_resources:
                yield pdf
            else:
                # Return empty DataFrame with proper schema if no resources
                yield pd.DataFrame()

    return transform


def generate_schema(transformer: FhirResourceTransformer) -> StructType:
    fields = [StructField(c.name, StringType(), True) for c in transformer.column_metadata()]
    return StructType(fields)


def main():
    RESOURCE_TYPE = "Patient"

    output_loc = f"s3a://spark-data/output/{RESOURCE_TYPE}"

    fhir_transformers = transformers[RESOURCE_TYPE]

    # 1. Initialize the session and connect to your Docker Master
    spark = (
        SparkSession.builder.appName("MyFirstClusterJob")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minio")
        .config("spark.hadoop.fs.s3a.secret.key", "minio123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # THE FIX: Force these to be pure integers (milliseconds)
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000")
        .config("spark.hadoop.fs.s3a.connection.request.timeout", "60000")
        .config("spark.hadoop.fs.s3a.socket.timeout", "60000")
        # Add this to prevent any "keepalive" string issues
        .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60")
        .config("spark.hadoop.fs.s3a.committer.name", "magic")
        .config("spark.hadoop.fs.s3a.committer.magic.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    files = glob.glob(f"/opt/spark/data/input/{RESOURCE_TYPE}/*.ndjson")

    # Read Data
    df = spark.read.option("multiLine", "false").json(files)

    print("Initial partitions:", df.rdd.getNumPartitions())

    # TODO: Check if needed
    _ = df.cache()

    # Transform Resource [list[dict]] using Fhir Transformer
    subtype_dfs = {}
    for transformers_cls in fhir_transformers:
        t = transformers_cls()
        table_name = generate_table_name(t.resource_type, t.resource_subtype)
        schema = generate_schema(t)

        subtype_dfs[table_name] = df.mapInPandas(transform_resources(t), schema=schema)

    print("df explain")
    df.explain(True)

    # Writes all csvs
    for table_name, subtype_df in subtype_dfs.items():
        output_path = f"{output_loc}/{table_name}"
        subtype_df.write.csv(output_path, mode="overwrite", header=True)

    print("subtype_df explain")
    subtype_df.explain(True)

    # d = subtype_dfs["patient"]
    #    d.show(5, truncate=False)

    print(f"Patients loaded {df.count()}")

    # 4. Show the result
    # df.show()

    # 5. Stop the session
    spark.stop()


if __name__ == "__main__":
    main()
