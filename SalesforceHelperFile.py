import pandas as pd
import numpy as np
import json
from simple_salesforce import Salesforce
from simple_salesforce.login import SalesforceLogin
from simple_salesforce.exceptions import SalesforceError


def SalesforceQuery(sf_json: str,
                    dataframe_header_name: str,
                    query_soql: str) -> pd.DataFrame:
    """Runs a SOQL query and returns a dataframe"""
    dataframe_header_name = {'Header': dataframe_header_name}
    querySOQL = query_soql
    df_output = pd.DataFrame()

    loginInfo = json.loads(sf_json)
    username = loginInfo['username']
    password = loginInfo['password']
    security_token = loginInfo['security_token']
    domain = 'login'

    try:
        session_id, instance = SalesforceLogin(username=username,
                                               password=password,
                                               security_token=security_token,
                                               domain=domain
                                               )
        sf = Salesforce(instance=instance, session_id=session_id)
    except SalesforceError as e:
        raise e

    if session_id == '':
        print(dataframe_header_name['Header'] + " session not established")
    else:
        print(dataframe_header_name['Header'] + " session established")

    try:
        response = sf.query(querySOQL)
    except SalesforceError as e:
        raise e
    listRecords = response.get('records')
    nextRecordsUrl = response.get('nextRecordsUrl')

    while not response.get('done'):
        response = sf.query_more(nextRecordsUrl, identifier_is_url=True)
        listRecords.extend(response.get('records'))
        nextRecordsUrl = response.get('nextRecordsUrl')

    if listRecords == '':
        print(dataframe_header_name['Header'] + " query returned no data")
    else:
        print(dataframe_header_name['Header'] + " query completed")

    df_output = pd.DataFrame(listRecords)
    if 'attributes' in df_output.columns:
        df_output.drop(labels=['attributes'], axis=1, inplace=True)
    return df_output


def sf_df_cleanup(df: pd.DataFrame) -> pd.DataFrame:
    """Flattens relationship columns with nested dictionaries and replaces them in the dataframe"""
    listColumns = list(df.columns)
    for col in listColumns:
        if any(isinstance(df[col].values[i], dict) for i in range(0, len(df[col].values))):
            if 'attributes' not in listColumns:
                df = pd.concat(
                    [df.drop(columns=[col]), df[col].apply(pd.Series).add_prefix(col + '.')],
                    axis=1)
            else:
                df = pd.concat(
                    [df.drop(columns=[col]), df[col].apply(pd.Series).drop('attributes', axis=1).add_prefix(col + '.')],
                    axis=1)
            new_columns = np.setdiff1d(df.columns, listColumns)
            for i in new_columns:
                listColumns.append(i)
    return df

