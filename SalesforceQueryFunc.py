

def SalesforceQuery(sf_json: str,
                    dataframe_header_name: str,
                    querySOQL:str):
    """Runs a SOQL query and returns a dataframe"""
    import json
    import pandas as pd
    from simple_salesforce import Salesforce, SalesforceLogin

    login_location=sf_json
    dataframe_header_name = {'Header':dataframe_header_name}
    querySOQL = querySOQL
    df_output = pd.DataFrame()

    loginInfo = json.load(open(login_location))
    username = loginInfo['username']
    password = loginInfo['password']
    security_token = loginInfo['security_token']
    domain = 'login'

    session_id, instance = SalesforceLogin(username=username,
                                           password=password,
                                           security_token=security_token,
                                           domain=domain)
    sf = Salesforce(instance=instance, session_id=session_id)

    if session_id == '':
        print(dataframe_header_name['Header']+" session not established")
    else:
        print(dataframe_header_name['Header']+" session established")
    
    records = sf.query(querySOQL)

    response = records = sf.query(querySOQL)
    listRecords = response.get('records')
    nextRecordsUrl = response.get('nextRecordsUrl')

    while not response.get('done'):
        response = sf.query_more(nextRecordsUrl, identifier_is_url=True)
        listRecords.extend(response.get('records'))
        nextRecordsUrl = response.get('nextRecordsUrl')

    if listRecords == '':
        print(dataframe_header_name['Header']+" query returned no data")
    else:
        print(dataframe_header_name['Header']+" query completed")

    df_output = pd.DataFrame(listRecords)
    return df_output

