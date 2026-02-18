import glob
import os
import re
import shutil
from collections.abc import Iterator

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import DataType, StringType, StructField, StructType
from radiant_fhir_transform_cli.transform.classes import transformers
from radiant_fhir_transform_cli.transform.classes.base import FhirResourceTransformer

# def transform_resources(resources: list[dict], fhir_transformers: dict[str, str]) -> list[dict]:
#    rows_by_table = {}
#    for table_name, fhir_transformer in fhir_transformers.items():
#        rows = fhir_transformer.transform_resources(resources)
#        rows_by_table[table_name] = rows
#
#    return rows_by_table


def camel_to_snake(name: str) -> str:
    """
    Converts a CamelCase or camelCase string to snake_case.

    Args:
        name: The CamelCase string.

    Returns:
        str: The converted snake_case string.
    """
    # Add underscore before uppercase letters (excluding first char)
    s1 = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)

    # Handle multiple uppercase sequences (e.g. JSONParser -> json_parser)
    return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


def generate_table_name(resource_type: str, resource_subtype: str | None) -> str:
    """
    Generate standardized table name for FHIR resource or subtype.

    Creates an Athena-compatible table name by converting CamelCase FHIR
    resource types to snake_case and optionally appending resource subtype.
    This ensures consistent naming conventions across all FHIR tables.

    Args:
        resource_type (str): FHIR resource type in CamelCase format
            (e.g., 'Patient', 'Observation', 'MedicationRequest').
        resource_subtype (str | None): Optional resource subtype in
            CamelCase format (e.g., 'VitalSigns', 'DiagnosticReport').
            If None, only resource type is used.

    Returns:
        str: Snake_case table name suitable for Athena. Format is either
            '{resource_type}' or '{resource_type}_{resource_subtype}'.
    """
    table_name = camel_to_snake(resource_type)

    if resource_subtype:
        table_name = f"{table_name}_{camel_to_snake(resource_subtype)}"

    return table_name


def transform_resources(transformer: FhirResourceTransformer):
    def transform(df_iter):
        for df in df_iter:
            # Convert DataFrame to list of dictionaries (resources)
            resources = df.to_dict("records")

            # Transform resources using the FHIR transformer
            transformed_resources = transformer.transform_resources(resources)

            # Convert back to DataFrame for Spark
            if transformed_resources:
                yield pd.DataFrame(transformed_resources)
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
        # Fix for the "60s" NumberFormatException:
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
        .config("spark.hadoop.fs.s3a.socket.timeout", "60000")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    files = glob.glob(f"/opt/spark/data/input/{RESOURCE_TYPE}/*.ndjson")

    # Read Data
    df = spark.read.option("multiLine", "false").json(files)

    _ = df.cache()

    # Transform Resource [list[dict]] using Fhir Transformer
    subtype_dfs = {}
    for transformers_cls in fhir_transformers:
        t = transformers_cls()
        table_name = generate_table_name(t.resource_type, t.resource_subtype)
        schema = generate_schema(t)

        subtype_dfs[table_name] = df.mapInPandas(transform_resources(t), schema=schema)

        # for table_name, subtype_df in subtype_dfs.items():
        # output_path = f"{output_loc}/{table_name}"
        # try:
        #    subtype_df.write.mode("overwrite").option("header", "true").option("escape", '"').csv(
        #        output_path
        #    )
        # except Exception as e:
        #    print(f"Error writing {table_name}: {e}")
        #    subtype_df.show()

    d = subtype_dfs["patient"]
    d.show(5, truncate=False)
    print(f"Patients loaded {df.count()}")

    # 4. Show the result
    # df.show()

    # 5. Stop the session
    spark.stop()


if __name__ == "__main__":
    main()
