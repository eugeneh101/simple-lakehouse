import aws_cdk as cdk

from simple_lakehouse import SimpleLakehouseStack


app = cdk.App()
environment = app.node.try_get_context("environment")
SimpleLakehouseStack(
    app,
    "SimpleLakehouseStack",
    environment=environment,
    env=cdk.Environment(region=environment["AWS_REGION"]),
)
app.synth()
