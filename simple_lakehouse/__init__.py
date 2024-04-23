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
            # s3_prefix="year=2024/month=04/",  ### put this back in
            database=self.simple_database,
            partition_keys=[  # data not in S3 file but in the Glue table as last columns
                glue.Column(name="year", type=glue.Schema.STRING),
                glue.Column(name="month", type=glue.Schema.STRING),
            ],
            columns=[  # need column order to be retained; data does not have header
                glue.Column(name="id", type=glue.Schema.SMALL_INT),
                glue.Column(name="first_name", type=glue.Schema.STRING),
                glue.Column(name="last_name", type=glue.Schema.STRING),
                glue.Column(name="age", type=glue.Schema.SMALL_INT),
                glue.Column(name="email", type=glue.Schema.STRING),
                glue.Column(name="ip_address", type=glue.Schema.STRING),
            ],
            data_format=glue.DataFormat.CSV,
            enable_partition_filtering=True,
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
                ],
            )
        )


        # connect AWS resources together
        self.upload_s3_files = s3_deploy.BucketDeployment(  # upload dags to S3
            self,
            "UploadS3Files",
            destination_bucket=self.s3_bucket,
            destination_key_prefix=f"year={environment['YEAR']}/month={environment['MONTH']}/",
            sources=[s3_deploy.Source.asset("./data")],  # hard coded
            prune=True,  ### it seems that delete Lambda uses a different IAM role
            retain_on_delete=False,
        )
        self.trigger_create_glue_table_partition_lambda = triggers.Trigger(
            self,
            "TriggerCreateGlueTablePartitionLambda",
            handler=self.create_glue_table_partition_lambda,  # this is underlying Lambda
            timeout=self.create_glue_table_partition_lambda.timeout,
            # invocation_type=triggers.InvocationType.REQUEST_RESPONSE,
            execute_after=[self.csv_table],
            execute_before=[],
        )
