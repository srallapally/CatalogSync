import csv
import math

import requests
from typing import Dict, Any, List

def get_access_token(token_url: str, client_id: str, client_secret: str) -> str:
    """
    Obtain an access token using client credentials flow.
    """
    data = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,
        'scope': 'fr:idm:* fr:iga:*'
    }
    response = requests.post(token_url, data=data)
    response.raise_for_status()
    return response.json()['access_token']

def fetch_data_from_api(api_url: str, search_string:str, access_token: str,page:str) -> Dict[str, Any]:
    api_url = f"{api_url}&pageNumber={page}"
    """
    Fetch JSON data from the provided API URL using the access token.
    """
    headers = {'Authorization': f'Bearer {access_token}'}
    body = {
            "targetFilter": {
                "operator": "OR",
                "operand": [
                    {
                        "operator": "CONTAINS",
                        "operand": {
                            "targetName": "application.name",
                            "targetValue": f"{search_string}"
                        }
                    },
                    {
                        "operator": "CONTAINS",
                        "operand": {
                            "targetName": "entitlementOwner.givenName",
                            "targetValue": f"{search_string}"
                        }
                    },
                    {
                        "operator": "CONTAINS",
                        "operand": {
                            "targetName": "descriptor.idx./entitlement.displayName",
                            "targetValue": f"{search_string}"
                        }
                    }
                ]
            }
    }

    response = requests.post(api_url, headers=headers, json=body)
    #print(response.json())
    response.raise_for_status()
    return response.json()

def extract_entitlement_data(result):
    try:
        entitlement = result['glossary']['idx']['/entitlement']
        return {
            'id': result.get('id', 'Unknown'),
            'glossary': entitlement
        }
    except KeyError:
        return {
            'id': result.get('id', 'Unknown'),
            'entitlement': None
        }


def write_to_csv(data: List[Dict[str, Any]], output_file: str, attribute_string: str):
    """
    Write the extracted data to a CSV file.
    """
    headers = ['id'] + attribute_string.split(',')

    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=headers, quoting=csv.QUOTE_ALL)
        writer.writeheader()

        for item in data:
            row = {'id': item['id']}
            glossary = item.get('glossary', {})
            for attr in headers[1:]:  # Skip 'id' as it's already added
                row[attr] = glossary.get(attr, '')
            writer.writerow(row)

def main():
    # Configuration
    base_url = "<CHANGE_ME>"
    token_url = f"{base_url}/am/oauth2/alpha/access_token"
    entitlement_url = f"{base_url}/iga/governance/resource/search?_fields=id,glossary,objGlossary&pageSize=10&sortBy=application.name&sortDir=desc"

    client_id = "<CHANGE_ME>"
    client_secret = "<CHANGE_ME>"
    output_file = "entitlement_export.csv"
    attribute_string = "isSensitive,is_privileged,lob_owner,requestable,approverRole,certFreq,classification,description,entitlementOwner"
    try:
        # Get access token
        access_token = get_access_token(token_url, client_id, client_secret)
        access_token = "<CHANGE ME>"
        # Fetch data from API)
        data = fetch_data_from_api(entitlement_url, "SNOW", access_token,0)
        # Initialize a list to store all items

        all_items_glossary = [extract_entitlement_data(result) for result in data.get("result", [])]

        total_rows = data["totalCount"]
        rows_per_page = 10
        pages = math.ceil(total_rows / rows_per_page)
        # Iterate through remaining pages
        for page in range(1, pages):
            # Adjust the URL or parameters for pagination
            paginated_data = fetch_data_from_api(entitlement_url, "SNOW", access_token, page=page)
            # Append new items to the all_items list
            all_items_glossary.extend([extract_entitlement_data(result) for result in paginated_data.get("result", [])])

        # Write data to CSV
        write_to_csv(all_items_glossary, output_file, attribute_string)
        print(f"Data successfully exported to {output_file}")
    except requests.RequestException as e:
        print(f"Error fetching data from API: {e}")
    except KeyError as e:
        print(f"Error processing API response: {e}")
    except IOError as e:
        print(f"Error writing CSV file: {e}")

if __name__ == "__main__":
    main()