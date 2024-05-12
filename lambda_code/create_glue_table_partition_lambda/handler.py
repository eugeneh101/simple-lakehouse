import os

import boto3


GLUE_DATABASE = os.environ["GLUE_DATABASE"]
GLUE_CSV_TABLE = os.environ["GLUE_CSV_TABLE"]
GLUE_PARQUET_TABLE = os.environ["GLUE_PARQUET_TABLE"]
YEAR = os.environ["YEAR"]
MONTH = os.environ["MONTH"]

GLUE_CLIENT = boto3.client("glue")


def lambda_handler(event, context) -> None:
    for glue_table in [GLUE_CSV_TABLE, GLUE_PARQUET_TABLE]:
        table_definition = GLUE_CLIENT.get_table(
            # CatalogId="string",
            DatabaseName=GLUE_DATABASE,
            Name=glue_table,
        )
        storage_descriptor = table_definition["Table"]["StorageDescriptor"]
        storage_descriptor["Location"] += f"year={YEAR}/month={MONTH}/"
        parition_input = {
            "StorageDescriptor": storage_descriptor,
            "Values": [YEAR, MONTH],
        }
        response = GLUE_CLIENT.batch_create_partition(
            # CatalogId="string",
            DatabaseName=GLUE_DATABASE,
            TableName=glue_table,
            PartitionInputList=[parition_input],
        )
        assert not response["Errors"]
        print("response:", response)
