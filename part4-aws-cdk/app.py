#!/usr/bin/env python3
import aws_cdk as cdk
from part4_aws_cdk.part4_aws_cdk_stack import Part4AwsCdkStack

app = cdk.App()
Part4AwsCdkStack(app, "Part4AwsCdkStack")
app.synth()
