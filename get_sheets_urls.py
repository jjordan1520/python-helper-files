import os
import json
import re
import boto3
from aws.AWS_Get_Secrets import get_secret

from GoogleSheetsHelperFile import drive_file_modified_time
from SalesforceHelperFile import (SalesforceQuery, sf_df_cleanup)


cwd = os.getcwd()
aws_cred_file = 'aws\\aws_iam_creds.json'
aws_creds = json.load(open(f"{cwd}\\{aws_cred_file}"))
GoogleSheets_AWSsecret = 'google/jim/serviceacct'
SF_AWSsecret = 'salesforce/jim/creds'
# get secrets
GoogleSecret = get_secret(
    secret_id=GoogleSheets_AWSsecret,
    aws_credentials=aws_creds
)

SFSecret = get_secret(
    secret_id=SF_AWSsecret,
    aws_credentials=aws_creds
)

# query Salesforce project objects for PRR Urls, SF ids, & last update times
sf_query = """SELECT Id, PRR_Estimate_URL__c FROM Project__c WHERE PRR_Estimate_URL__c!=''"""

pattern_string = r'http[s]:\/?\/?[^:\/\s]+\/\w+\/d\/(.+)\/'
regex_pattern = re.compile(pattern=pattern_string)


sheets_urls = [
'https://docs.google.com/spreadsheets/d/1NvRfz3PaLuUisk6xFtXrLyCywJIuL1UIqjFW8dLitNU/edit?gid=239488602#gid=239488602',
'https://docs.google.com/spreadsheets/d/1PSq2uyMWmNk1G8cUw2_Qf5LJaIyugjg8DOPZP9SKKGU/edit?gid=239488602#gid=239488602',
'https://docs.google.com/spreadsheets/d/1mDMDhtlFW78MFXb_Gmb9qnSjKUmR1PUYx8uLLydrxvY/edit?gid=239488602#gid=239488602'
]
sheets_to_get = []

for url in sheets_urls:
    url_sheet_id = re.match(pattern=regex_pattern, string=url).groups()[0]
    # check last modified time for PRR file
    file_modifiedTime_response = drive_file_modified_time(
        service_account_file=GoogleSecret,
        file_id=url_sheet_id
    )
    print(url_sheet_id, file_modifiedTime_response)
    sheets_to_get.append(url_sheet_id)




