import requests
import json
import os

# HubSpot Private App Access Token (replace with your actual token)
HUBSPOT_ACCESS_TOKEN = os.getenv('HUBSPOT_ACCESS_TOKEN')

# Custom object API name in HubSpot for Premises
PREMISES_OBJECT_API_NAME = "2-34057446"  # Update with the actual API name of your custom object

# Headers for HubSpot API requests
HUBSPOT_HEADERS = {
    "Authorization": f"Bearer {HUBSPOT_ACCESS_TOKEN}",
    "Content-Type": "application/json"
}

# Load premises data from JSON file
def load_premises_data(filename="enriched_premises_data.json"):
    with open(filename, 'r') as json_file:
        return json.load(json_file)

# Check if a premises custom object exists in HubSpot using its premise_id
def find_existing_premises(premise_id):
    url = f"https://api.hubapi.com/crm/v3/objects/{PREMISES_OBJECT_API_NAME}/search"
    query = {
        "filterGroups": [{
            "filters": [{
                "propertyName": "premise_id",
                "operator": "EQ",
                "value": premise_id
            }]
        }]
    }

    response = requests.post(url, headers=HUBSPOT_HEADERS, json=query)
    
    if response.status_code == 200:
        data = response.json()
        if data['results']:
            return data['results'][0]['id']  # Return the existing premises ID in HubSpot
        return None
    else:
        print(f"Error searching for premise: {response.text}")
        return None

# Create a new premises custom object in HubSpot
def create_premises(premise):
    url = f"https://api.hubapi.com/crm/v3/objects/{PREMISES_OBJECT_API_NAME}"

    # Address should be street number and street name
    address = f"{premise.get('street_number', '')} {premise.get('street_name', '')}"

    premises_data = {
        "properties": {
            "premise_id": premise.get('id'),  # Store the premise ID as premise_id in HubSpot
            "city": premise.get('city', ''),
            "state": premise.get('province', ''),
            "postal_code": premise.get('postal_code', ''),
            "latitude": premise.get('latitude', ''),
            "longitude": premise.get('longitude', ''),
            "status": premise.get('status', ''),
            "address": address  # Use street number and street name as address
        }
    }

    response = requests.post(url, headers=HUBSPOT_HEADERS, json=premises_data)

    if response.status_code == 201:
        print(f"Premises {premise.get('id')} created successfully.")
    else:
        print(f"Error creating premises: {response.text}")

# Update an existing premises custom object in HubSpot
def update_premises(premises_id, premise):
    url = f"https://api.hubapi.com/crm/v3/objects/{PREMISES_OBJECT_API_NAME}/{premises_id}"

    # Address should be street number and street name
    address = f"{premise.get('street_number', '')} {premise.get('street_name', '')}"

    premises_data = {
        "properties": {
            "city": premise.get('city', ''),
            "state": premise.get('province', ''),
            "postal_code": premise.get('postal_code', ''),
            "latitude": premise.get('latitude', ''),
            "longitude": premise.get('longitude', ''),
            "status": premise.get('status', ''),
            "address": address  # Use street number and street name as address
        }
    }

    response = requests.patch(url, headers=HUBSPOT_HEADERS, json=premises_data)

    if response.status_code == 200:
        print(f"Premises {premise.get('id')} updated successfully.")
    else:
        print(f"Error updating premises {premises_id}: {response.text}")

# Process premises data to create or update premises in HubSpot
def process_premises():
    premises_data = load_premises_data()

    for premise in premises_data:
        premise_id = premise.get('id')  # Use the 'id' from the premises data as 'premise_id' in HubSpot

        # Check if the premises already exists in HubSpot
        existing_premises_id = find_existing_premises(premise_id)

        if existing_premises_id:
            # Update the existing premises custom object
            update_premises(existing_premises_id, premise)
        else:
            # Create a new premises custom object
            create_premises(premise)

# Run the main function
if __name__ == "__main__":
    process_premises()
