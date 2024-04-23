import os

import boto3


GLUE_DATABASE = os.environ["GLUE_DATABASE"]
GLUE_CSV_TABLE = os.environ["GLUE_CSV_TABLE"]
YEAR = os.environ["YEAR"]
MONTH = os.environ["MONTH"]

GLUE_CLIENT = boto3.client("glue")


def lambda_handler(event, context) -> None:
    table_definition = GLUE_CLIENT.get_table(
        # CatalogId='string',
        DatabaseName=GLUE_DATABASE,
        Name=GLUE_CSV_TABLE,
    )
    storage_descriptor = table_definition["Table"]["StorageDescriptor"]
    storage_descriptor["Location"] += f"year={YEAR}/month={MONTH}/"
    parition_input = {"StorageDescriptor": storage_descriptor, "Values": [YEAR, MONTH]}

    response = GLUE_CLIENT.batch_create_partition(
        # CatalogId='string',
        DatabaseName=GLUE_DATABASE,
        TableName=GLUE_CSV_TABLE,
        PartitionInputList=[parition_input],
    )
    assert not response["Errors"]
    print("response:", response)
