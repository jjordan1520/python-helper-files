# function to get AWS secrets

import boto3
from botocore.exceptions import ClientError


def get_secret(
        secret_id: str,
        aws_credentials: dict = None
) -> dict | str:
    """Helper function that gets AWS Secret from AWS Secrets Manager and returns it in a string format."""
    secret_name = secret_id
    region_name = "us-east-1"

    # Create a Secrets Manager client
    # comment out args if publishing to lambda
    if aws_credentials is not None:
        session = boto3.session.Session(
            aws_access_key_id=aws_credentials['Access key'],
            aws_secret_access_key=aws_credentials['Secret access key']
        )
    else:
        session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    secret = get_secret_value_response['SecretString']
    return secret


# # testing
# if __name__ == '__main__':
#     import json
#
#     aws_creds = json.load(open('aws_iam_creds.json'))
#     GoogleSheets_AWSsecret = 'google/jim/serviceacct'
#
#     try:
#         GoogleSecret = get_secret(
#             secret_id=GoogleSheets_AWSsecret,
#             aws_credentials=aws_creds
#         )  # google auth accepts secret string
#         print(GoogleSecret)
#     except Exception as e:
#         print(f"There was a problem retrieving the AWS Secret {GoogleSheets_AWSsecret}\n"
#               f"Error: {e}")


