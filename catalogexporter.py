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

def fetch_glossary_schema(api_url: str, token_url: str, client_id: str, client_secret: str) -> Dict[str, Any]:
    """
    Fetch JSON data from the provided API URL using client credentials flow.
    """
    # Get the access token
    access_token = get_access_token(token_url, client_id, client_secret)
    # Use the access token to fetch data from the API
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    body = {
        "targetFilter": {
            "operator": "AND",
            "operand": [
                {
                    "operator": "EQUALS",
                    "operand": {
                        "targetName": "objectType",
                        "targetValue": "/openidm/managed/assignment"
                    }
                }
            ]
        }
    }
    response = requests.post(api_url, headers=headers, json=body)
    response.raise_for_status()  # Raise an exception for HTTP errors
    return response.json()

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
    #print(api_url)
    response = requests.post(api_url, headers=headers, json=body)
    response.raise_for_status()
    return response.json()

def flatten_glossary(glossary: Dict[str, Any]) -> Dict[str, Any]:
    """Flatten nested glossary objects."""
    flat = {}
    for key, value in glossary.items():
        if isinstance(value, dict):
            for sub_key, sub_value in value.items():
                flat[f"{key}_{sub_key}"] = sub_value if sub_value is not None and sub_value != "" else ""
        else:
            flat[key] = value if value is not None and value != "" else ""
    return flat

def get_all_headers(data: List[Dict[str, Any]]) -> List[str]:
    """Get all unique headers from the data."""
    headers = set(["id"])
    for item in data:
        if "objGlossary" in item:
            headers.update(flatten_glossary(item["objGlossary"]).keys())
    return sorted(list(headers))

def process_headers_data(json_data: Dict[str, Any]) -> List[List[str]]:
    """
    Process the JSON data and return headers and sample data.
    """
    if "result" not in json_data or not isinstance(json_data["result"], list):
        raise ValueError("Invalid JSON structure: 'result' key not found or not a list")

    headers = [field["name"] for field in json_data["result"]]
    return headers

def main():
    # Configuration
    base_url = "https://<CHANGE ME>.forgeblocks.com"
    token_url = f"{base_url}/am/oauth2/alpha/access_token"
    entitlement_url = f"{base_url}/iga/governance/resource/search?pageSize=10&sortBy=application.name&sortDir=desc"
    glossary_schema_url = f"{base_url}/iga/commons/glossary/schema/search?pageNumber=0&pageSize=10&sortBy=name&sortDir=asc"
    client_id = "<CHANGE ME>"
    client_secret = "<CHANGE ME>"
    output_file = "<CHANGE ME>"

    try:
        # Get access token
        access_token = get_access_token(token_url, client_id, client_secret)

        # Fetch data from API
        data = fetch_data_from_api(entitlement_url, "SNOW", access_token,0)
        # Initialize a list to store all items
        all_items = data.get("result", [])
        total_rows = data["totalCount"]
        rows_per_page = 10
        pages = math.ceil(total_rows / rows_per_page)
        # Iterate through remaining pages
        for page in range(1, pages):
            # Adjust the URL or parameters for pagination
            paginated_data = fetch_data_from_api(entitlement_url, "SNOW", access_token, page=page)
            # Append new items to the all_items list
            all_items.extend(paginated_data.get("result", []))

        # Get all headers
        headers = get_all_headers(data["result"])

        entitlement_glossary_schema = fetch_glossary_schema(glossary_schema_url, token_url, client_id, client_secret)
        entitlement_headers = process_headers_data(entitlement_glossary_schema)
        # Write to CSV
        with open(output_file, 'w', newline='') as f:
            all_headers = headers + entitlement_headers
            writer = csv.DictWriter(f, fieldnames=all_headers, quoting=csv.QUOTE_ALL)
            writer.writeheader()  # Write header only if the file is newly created
            for item in all_items:
                row = {header: "" for header in all_headers}
                row["id"] = item.get("id")
                if "objGlossary" in item and item["objGlossary"]:
                    flattened = flatten_glossary(item["objGlossary"])
                    row.update({k: str(v) if v is not None else "" for k, v in flattened.items() if k in all_headers})
                writer.writerow(row)

        print(f"CSV file '{output_file}' has been generated successfully.")

    except requests.RequestException as e:
        print(f"Error fetching data from API: {e}")
    except KeyError as e:
        print(f"Error processing API response: {e}")
    except IOError as e:
        print(f"Error writing CSV file: {e}")

if __name__ == "__main__":
    main()