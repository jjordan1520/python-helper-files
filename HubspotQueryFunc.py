###
# Hubspot query for contacts/leads statuses
# Pulls all hubspot contact
# If properties with history contains "lifecyclestage"
# or "hs_latest_source_data_1" flag for MQL status or concat latest source data


import hubspot
from hubspot.crm.contacts import ApiException
import json
import pandas as pd
import numpy as np


def hubspotquery(hs_keyfile: str,
                 properties_to_get: list,
                 properties_with_history_to_get: list):
    """Queries Hubspot contacts API and returns a dataframe with Hubspot properties;
     Iterates through historic contact statuses to add flag if ever has been MQL & date of last MQL status"""

    hs_keyfile_location = json.load(open(hs_keyfile))
    hubspot_key = hs_keyfile_location['key']

    client = hubspot.Client.create(access_token=hubspot_key)
    properties_to_get = properties_to_get
    properties_with_history_to_get = properties_with_history_to_get

    if client == '':
        print("Hubspot session not established.")
    else:
        print("Hubspot query started. This may take a couple minutes.")

    try:
        api_response = client.crm.contacts.basic_api.get_page(limit=50,
                                                              properties=properties_to_get,
                                                              properties_with_history=properties_with_history_to_get,
                                                              archived=False)
        api_results = api_response.results
        after = api_response.paging.next.after

        while after:
            api_response_next = client.crm.contacts.basic_api.get_page(limit=50,
                                                                       after=after,
                                                                       properties=properties_to_get,
                                                                       properties_with_history=properties_with_history_to_get,
                                                                       archived=False)
            api_results_next = api_response_next.results
            api_results.extend(api_results_next)
            if not hasattr(api_response_next.paging, "next"):
                break
            after = api_response_next.paging.next.after

    except ApiException as e:
        print("Exception when calling basic_api->get_page: %s\n" % e)

    # Empty dataframe to house api results
    df_api_results = pd.DataFrame()

    # Iterate through api response properties, adding each value to dataframe
    for i in range(0, len(api_results)):
        df_api_results = pd.concat([df_api_results, pd.DataFrame(api_results[i].properties, index=[i])], axis=0)

    # Empty dataframe to house api results history
    df_api_results['MQLFlag'] = ''
    df_api_results['MQLDate'] = ''
    tradeshowsitelist = []
    df_api_results['TradeShowInfluence'] = ''

    # for each record in api results, check each "properties with history" record for MQL lifecycle stage flag
    if 'lifecyclestage' in properties_with_history_to_get:
        for i in range(0, len(df_api_results)):
            for j in range(0, len(api_results[i].properties_with_history['lifecyclestage'])):
                if 'marketingqualifiedlead' in api_results[i].properties_with_history['lifecyclestage'][j].value:
                    df_api_results['MQLDate'][i] = api_results[i].properties_with_history['lifecyclestage'][j].timestamp
                    df_api_results['MQLFlag'][i] = 'marketingqualifiedlead' in api_results[i].properties_with_history['lifecyclestage'][j].value

    # for each record in api results, check each properties with history record for hs_latest_source_data_1
    if 'hs_latest_source_data_1' in properties_with_history_to_get:
        for i in range(0, len(df_api_results)):
            for j in range(0, len(api_results[i].properties_with_history['hs_latest_source_data_1'])):
                if 'ossovr-4656158.hs-sites.com' in api_results[i].properties_with_history['hs_latest_source_data_1'][j].value:
                    tradeshowsitelist.append(api_results[i].properties_with_history['hs_latest_source_data_1'][j].value)
                    df_api_results['TradeShowInfluence'][i] = str(",".join(set(tradeshowsitelist)))
            # reset list for next iteration
            tradeshowsitelist = []

    df_api_results['MatchField'] = np.where(df_api_results['salesforcecontactid'].isnull(),
                                            df_api_results['salesforceleadid'],
                                            df_api_results['salesforcecontactid'])
    print("Hubspot query completed.")
    return df_api_results
