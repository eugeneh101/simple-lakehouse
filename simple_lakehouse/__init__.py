from aws_cdk import (
    RemovalPolicy,
    Stack,
    aws_iam as iam,
    aws_lambda as _lambda,
    aws_s3 as s3,
    aws_s3_deployment as s3_deploy,
    triggers,
)
from aws_cdk import aws_glue_alpha as glue
from constructs import Construct


class SimpleLakehouseStack(Stack):
    def __init__(
        self, scope: Construct, construct_id: str, environment: dict, **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.s3_bucket = s3.Bucket(
            self,
            "S3BucketSource",
            bucket_name=environment["S3_BUCKET"],
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        self.simple_database = glue.Database(
            self, "SimpleDatabase", database_name=environment["GLUE_DATABASE"]
        )

        self.csv_table = glue.S3Table(
            self,
            "CsvTable",
            table_name=environment["GLUE_CSV_TABLE"],
            bucket=self.s3_bucket,
            s3_prefix=f"{environment['S3_CSV_FOLDER']}/",
            database=self.simple_database,
            partition_keys=[  # data not in S3 file but in the Glue table as last columns
                glue.Column(name="year", type=glue.Schema.STRING),
                glue.Column(name="month", type=glue.Schema.STRING),
            ],
            columns=[  # need column order to be retained; data does not have header
                glue.Column(
                    name="id", type=glue.Schema.BIG_INT
                ),  # automatically truncates float to int
                glue.Column(name="first_name", type=glue.Schema.STRING),
                glue.Column(name="email", type=glue.Schema.STRING),
                glue.Column(name="age", type=glue.Schema.SMALL_INT),
                glue.Column(name="height", type=glue.Schema.FLOAT),
                glue.Column(name="married", type=glue.Schema.BOOLEAN),
                glue.Column(name="registration_date", type=glue.Schema.DATE),
                glue.Column(name="purchase_time", type=glue.Schema.TIMESTAMP),
            ],
            data_format=glue.DataFormat.CSV,
            enable_partition_filtering=True,
            # parameters={},  # shows up in table properties, not serde parameters
            # partition_indexes=None, compressed=None, description=None,
        )
        self.csv_table.node.default_child.add_property_override(  # CDK raw override
            "TableInput.StorageDescriptor.SerdeInfo",
            {  # this serializer can handle missing values which it replaces with NULLs
                "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                "Parameters": {
                    "field.delim": ",",  # "separatorChar": ",",
                    "skip.header.line.count": "1",
                    # "serialization.format": 1,  # have no idea what this does
                },
            },
        )

        self.parquet_table = glue.S3Table(
            self,
            "ParquetTable",
            table_name=environment["GLUE_PARQUET_TABLE"],
            bucket=self.s3_bucket,
            s3_prefix=f"{environment['S3_PARQUET_FOLDER']}/",
            database=self.simple_database,
            partition_keys=[  # data not in S3 file but in the Glue table as last columns
                glue.Column(name="year", type=glue.Schema.STRING),
                glue.Column(name="month", type=glue.Schema.STRING),
            ],
            columns=[
                glue.Column(name="id", type=glue.Schema.BIG_INT),
                glue.Column(name="first_name", type=glue.Schema.STRING),
                glue.Column(name="email", type=glue.Schema.STRING),
                glue.Column(name="age", type=glue.Schema.SMALL_INT),
                glue.Column(name="height", type=glue.Schema.DOUBLE),
                glue.Column(name="married", type=glue.Schema.BOOLEAN),
                # cast before saving to parquet: df["registration_date"] = pd.to_datetime(df["registration_date"]).dt.date
                glue.Column(name="registration_date", type=glue.Schema.DATE),
                # cast before saving to parquet: df["purchase_time"] = pd.to_datetime(df["purchase_time"])
                glue.Column(name="purchase_time", type=glue.Schema.TIMESTAMP),
            ],
            data_format=glue.DataFormat.PARQUET,
            enable_partition_filtering=True,
            # parameters={},  # shows up in table properties, not serde parameters
            # partition_indexes=None, compressed=None, description=None,
        )

        # will be used once in Trigger defined below
        self.create_glue_table_partition_lambda = _lambda.Function(
            self,
            "CreateGlueTablePartitionLambda",
            function_name=environment["CREATE_GLUE_TABLE_PARTITION_LAMBDA"],
            runtime=_lambda.Runtime.PYTHON_3_10,
            code=_lambda.Code.from_asset(
                "lambda_code/create_glue_table_partition_lambda",
                exclude=[".venv/*"],
            ),
            handler="handler.lambda_handler",
            environment={
                "GLUE_DATABASE": environment["GLUE_DATABASE"],
                "GLUE_CSV_TABLE": environment["GLUE_CSV_TABLE"],
                "GLUE_PARQUET_TABLE": environment["GLUE_PARQUET_TABLE"],
                "YEAR": environment["YEAR"],
                "MONTH": environment["MONTH"],
            },
        )
        self.create_glue_table_partition_lambda.add_to_role_policy(
            statement=iam.PolicyStatement(
                actions=["glue:GetTable", "glue:BatchCreatePartition"],
                resources=[
                    f"arn:aws:glue:{environment['AWS_REGION']}:{self.account}:catalog",
                    self.simple_database.database_arn,
                    self.csv_table.table_arn,
                    self.parquet_table.table_arn,
                ],
            )
        )

        # connect AWS resources together
        self.upload_csv_files_to_s3 = s3_deploy.BucketDeployment(  # upload dags to S3
            self,
            "UploadCsvFilesToS3",
            destination_bucket=self.s3_bucket,
            destination_key_prefix=(
                f"{environment['S3_CSV_FOLDER']}/"
                f"year={environment['YEAR']}/month={environment['MONTH']}/"
            ),
            sources=[
                s3_deploy.Source.asset(f"./data/{environment['S3_CSV_FOLDER']}")
            ],  # hard coded pattern
            prune=True,  ### it seems that delete Lambda uses a different IAM role
            retain_on_delete=False,
        )
        self.upload_parquet_files_to_s3 = (
            s3_deploy.BucketDeployment(  # upload dags to S3
                self,
                "UploadParquetFilesToS3",
                destination_bucket=self.s3_bucket,
                destination_key_prefix=(
                    f"{environment['S3_PARQUET_FOLDER']}/"
                    f"year={environment['YEAR']}/month={environment['MONTH']}/"
                ),
                sources=[
                    s3_deploy.Source.asset(f"./data/{environment['S3_PARQUET_FOLDER']}")
                ],  # hard coded pattern
                prune=True,  ### it seems that delete Lambda uses a different IAM role
                retain_on_delete=False,
            )
        )
        self.trigger_create_glue_table_partition_lambda = triggers.Trigger(
            self,  # might be possible to replace Trigger with CustomResource
            "TriggerCreateGlueTablePartitionLambda",
            handler=self.create_glue_table_partition_lambda,  # this is underlying Lambda
            timeout=self.create_glue_table_partition_lambda.timeout,
            # invocation_type=triggers.InvocationType.REQUEST_RESPONSE,
            execute_after=[self.csv_table],
            execute_before=[],
        )


### TODOs:
# figure out snappy compression
# see if BucketDeployment can upload into S3 as folders
# add parquet with NULLs
