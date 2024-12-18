import boto3
from botocore.exceptions import ClientError
from io import StringIO  # use to write dataframe to csv in memory before upload
import pandas as pd
from datetime import datetime
# import csv - not needed here; will convert to DataFrame in lambda_function.py


def aws_csv_from_s3(event) -> StringIO:
    """Helper function that reads a csv file when triggered by S3 event"""
    # establish aws s3 session and read csv object into dataframe

    s3 = boto3.client('s3')

    bucket_name = event['Records'][0]['s3']['bucket']['name']
    object_key = event['Records'][0]['s3']['object']['key']
    try:
        response_object = s3.get_object(Bucket=bucket_name, Key=object_key)
        raw_data = response_object['Body']._raw_stream.data.decode('utf-8')
    except ClientError as e:
        raise e

    data = StringIO(raw_data)
    # data = csv.DictReader(buf)
    # data_dict = list(reader)
    return data


def aws_csv_to_s3(
        incoming_data: pd.DataFrame,
        s3_bucket: str,
        filename_base: str,
        append_date: bool = True
):
    """Helper function that converts dataframe to csv and puts in S3 bucket."""
    csv_buffer = StringIO()
    incoming_data.to_csv(path_or_buf=csv_buffer, index=False)
    date = datetime.utcnow().strftime(format='%Y-%m-%d-%H%M%S')

    # Establish S3 session
    s3 = boto3.resource('s3')
    bucket_name = s3_bucket
    if append_date:
        try:
            response = s3.Object(bucket_name, f'{filename_base}-{date}.csv').put(Body=csv_buffer.getvalue())
        except ClientError as e:
            raise e
        else:
            print(f"CSV file written to {bucket_name} with entity tag: {response['ETag']}")
    else:
        try:
            response = s3.Object(bucket_name, f'{filename_base}.csv').put(Body=csv_buffer.getvalue())
        except ClientError as e:
            raise e
        else:
            print(f"CSV file written to {bucket_name} with entity tag: {response['ETag']}")


def aws_json_to_s3(
        incoming_data: pd.DataFrame, 
        s3_bucket: str,
        filename_base: str,
        append_date: bool = True
):
    """Helper function that converts incoming dataframe to json and puts it in S3 bucket."""
    json_buffer = StringIO()
    incoming_data.to_json(path_or_buf=json_buffer, orient='records')
    date = datetime.utcnow().strftime(format='%Y-%m-%d-%H%M%S')

    # Establish S3 session
    s3 = boto3.resource('s3')
    bucket_name = s3_bucket
    if append_date:
        try:
            response = s3.Object(bucket_name, f'{filename_base}-{date}.json').put(Body=json_buffer.getvalue())
        except ClientError as e:
            raise e
        else:
            print(f"json file written to {bucket_name} with entity tag: {response['ETag']}")
    else:
        try:
            response = s3.Object(bucket_name, f'{filename_base}.json').put(Body=json_buffer.getvalue())
        except ClientError as e:
            raise e
        else:
            print(f"json file written to {bucket_name} with entity tag: {response['ETag']}")





