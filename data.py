import os
import requests
import json
from datetime import datetime, timedelta

# Base URL for API
BASE_URL = "https://fno.national-us.aex.systems"

# Fetch API_TOKEN from environment or .env file
API_TOKEN = os.getenv('API_TOKEN')   # Fetching API token from environment variable

if not API_TOKEN:
    raise Exception("API_TOKEN environment variable is not set")

HEADERS = {
    "Authorization": f"Bearer {API_TOKEN}",
    "Content-Type": "application/json"
}

# Set the number of hours for 'updated_after'. If None, defaults to 24 hours.
HOURS = 5

# Function to get 'updated_after' date (24 hours prior or custom interval)
def get_updated_after(hours=None):
    # If hours is None, default to 24 hours
    if hours is None:
        hours = 24
    pull_time = datetime.now() - timedelta(hours=hours)
    formatted_time = pull_time.isoformat().replace('T', ' ').split('.')[0]
    return formatted_time

# Fetch premises with updated_after filter and handle pagination
def fetch_premises(updated_after, page=1):
    url = f"{BASE_URL}/premises"
    params = {
        "updated_after": updated_after,
        "page": page
    }

    try:
        response = requests.get(url, headers=HEADERS, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Error fetching premises (page {page}): {response.status_code}")
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

# Fetch services for each premise by premise_id and log response
def fetch_services(premise_id):
    url = f"{BASE_URL}/services?premise={premise_id}"  # Correctly passing the premise_id in the URL

    try:
        response = requests.get(url, headers=HEADERS)
        if response.status_code == 200:
            services_data = response.json()
            services = services_data.get('items', [])  # Extract the list of services from 'items'
            print(f"Services Data for Premise {premise_id}: {services_data}")  # Log the raw services data
            return services  # Return the list of services
        else:
            raise Exception(f"Error fetching services for premise {premise_id}: {response.status_code}")
    except Exception as e:
        print(f"An error occurred while fetching services: {e}")
        return None

# Fetch full service details and work orders by service_id
def fetch_service_details(service_id):
    full_service_url = f"{BASE_URL}/services/{service_id}/full"

    try:
        full_service_response = requests.get(full_service_url, headers=HEADERS)

        if full_service_response.status_code == 200:
            return full_service_response.json()
        else:
            raise Exception(f"Error fetching details for service {service_id}")
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

# Fetch work orders by service_id
def fetch_work_orders(service_id):
    url = f"{BASE_URL}/work-orders"
    params = {"service": service_id}  # Correctly passing the service_id

    try:
        response = requests.get(url, headers=HEADERS, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Error fetching work orders for service {service_id}: {response.status_code}")
    except Exception as e:
        print(f"An error occurred while fetching work orders: {e}")
        return None

# Fetch customer details by customer_id
def fetch_customer_details(customer_id):
    customer_url = f"{BASE_URL}/customers/{customer_id}"
    customer_services_url = f"{BASE_URL}/customers/{customer_id}/services"

    try:
        customer_response = requests.get(customer_url, headers=HEADERS)
        customer_services_response = requests.get(customer_services_url, headers=HEADERS)

        if customer_response.status_code == 200 and customer_services_response.status_code == 200:
            return {
                "customer_details": customer_response.json(),
                "customer_services": customer_services_response.json()
            }
        else:
            raise Exception(f"Error fetching details for customer {customer_id}")
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

# Function to loop through all pages and collect premises data
def fetch_all_premises(hours=None):
    updated_after_time = get_updated_after(hours)  # Fetch data updated within the specified number of hours
    all_premises = []
    current_page = 1

    while True:
        premises_data = fetch_premises(updated_after_time, page=current_page)

        if premises_data is None:
            print("No data found or an error occurred")
            break

        # Append the fetched items to the all_premises list
        all_premises.extend(premises_data['items'])

        total_items = premises_data['total']
        items_fetched = len(all_premises)

        print(f"Fetched {items_fetched} out of {total_items} total items")

        # Stop fetching if we have retrieved all items
        if items_fetched >= total_items:
            break

        current_page += 1

    return all_premises

# Enrich each premise with its services, work orders, and customer details
def enrich_premises_with_services_and_customers(premises_data):
    enriched_data = []
    for premise in premises_data:
        premise_id = premise['id']
        customer_id = premise['customer_id']

        # Fetch related services for this premise
        services = fetch_services(premise_id)

        # For each service, fetch detailed service info and work orders
        service_details = []
        if services and isinstance(services, list):  # Ensure services is a valid list
            for service in services:
                if isinstance(service, dict) and 'id' in service:
                    service_id = service['id']
                    
                    # Fetch detailed service info
                    details = fetch_service_details(service_id)
                    
                    # Fetch related work orders for the service
                    work_orders = fetch_work_orders(service_id)

                    # Attach work orders to the service details
                    service_info = {
                        "service_details": details,
                        "work_orders": work_orders
                    }
                    service_details.append(service_info)
                else:
                    print(f"Invalid service data for premise {premise_id}: {service}")
        else:
            print(f"No valid services found for premise {premise_id}")

        # Fetch customer details for this premise
        customer = fetch_customer_details(customer_id)

        # Attach services and customer info to the premise data
        premise['services'] = service_details
        premise['customer'] = customer
        enriched_data.append(premise)

    return enriched_data

# Save the enriched data to a JSON file (overwrites the file each time)
def save_data_to_file(data, filename="enriched_premises_data.json"):
    with open(filename, 'w') as json_file:
        json.dump(data, json_file, indent=4)
        print(f"Data saved to {filename}")

# Main function to demonstrate the API call with pagination and save enriched data to file
def main():
    all_premises_data = fetch_all_premises(HOURS)

    if all_premises_data:
        enriched_data = enrich_premises_with_services_and_customers(all_premises_data)
        save_data_to_file(enriched_data)
        print(f"Fetched and enriched {len(enriched_data)} premises in total.")
    else:
        print("No premises data available or an error occurred")

# Run the main function
if __name__ == "__main__":
    main()
