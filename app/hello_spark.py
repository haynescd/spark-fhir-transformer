import glob
import re
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
    def transform(itr: Iterator[pd.DataFrame]):
        for resource in itr:
            pass

        pass


def generate_schema(transformer: FhirResourceTransformer) -> DataType:
    return DataType.fromDDL(" ".join([f"{c.name} string," for c in transformer.column_metadata()]))


def main():
    RESOURCE_TYPE = "Patient"

    fhir_transformers = transformers[RESOURCE_TYPE]
    # 1. Initialize the session and connect to your Docker Master
    spark = SparkSession.builder.appName("MyFirstClusterJob").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    files = glob.glob(f"/opt/spark/data/{RESOURCE_TYPE}/*.ndjson")

    # Read Data
    df = spark.read.option("multiLine", "false").json(files)

    # Transform Resource [list[dict]] using Fhir Transformer
    subtype_dfs = {}
    for transformers_cls in fhir_transformers:
        t = transformers_cls()
        table_name = generate_table_name(t.resource_type, t.resource_subtype)
        schema = generate_schema(t)

        subtype_dfs[table_name] = df.mapInPandas(transform_resources(t), schema=schema)

    print(f"Patients loaded {df.count()}")

    # 4. Show the result
    # df.show()

    # 5. Stop the session
    spark.stop()


if __name__ == "__main__":
    main()
