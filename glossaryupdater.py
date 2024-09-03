import csv
import requests
from typing import List, Dict, Any
import json


def get_access_token(token_url: str, client_id: str, client_secret: str) -> str:
    """
    Obtain an access token using client credentials flow with specific scopes.
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

def is_row_empty(row):
    return all(value.strip() == "" for key, value in row.items() if key != "id")

def main():
    # Configuration
    base_url = "<CHANGE_ME>"
    token_url = f"{base_url}/am/oauth2/alpha/access_token"
    client_id = "<CHANGE_ME>"
    client_secret = "<CHANGE_ME>"
    csv_file = "entitlement_export.csv"

    # Get access token
    #access_token = get_access_token(token_url, client_id, client_secret)
    access_token = "<CHANGE ME>"
    # Read and process CSV file
    with open(csv_file, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if is_row_empty(row):
                print(f"Skipping update for {row['id']} as all other fields are empty")
                continue
            id = row["id"]
            glossary_update_url = f"{base_url}/iga/governance/resource/{id}/glossary"
            data = {
                "isSensitive": row["isSensitive"].lower(),
                "is_privileged": row["is_privileged"].lower(),
                "lob_owner": row["lob_owner"],
                "requestable": row["requestable"].lower(),
                "approverRole": row["approverRole"],
                "certFreq": row["certFreq"],
                "classification": row["classification"],
                "description": row["description"],
                "entitlementOwner": row["entitlementOwner"]
            }
            # Convert the data to a JSON string with double quotes
            json_data = json.dumps(data)

            # Make the PUT request
            headers = {'Content-Type': 'application/json','Authorization':f'Bearer {access_token}'}
            # Make the PUT request
            response = requests.put(f"{glossary_update_url}", json=data,headers=headers)

            # Check the response
            if response.status_code == 200:
                print(f"Successfully updated entitlement {row['id']}")
            else:
                print(f"Failed to update entitlement {row['id']}. Status code: {response.status_code}")


if __name__ == "__main__":
    main()