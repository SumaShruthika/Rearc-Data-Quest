import aws_cdk as core
import aws_cdk.assertions as assertions

from part4_aws_cdk.part4_aws_cdk_stack import Part4AwsCdkStack

# example tests. To run these tests, uncomment this file along with the example
# resource in part4_aws_cdk/part4_aws_cdk_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = Part4AwsCdkStack(app, "part4-aws-cdk")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
