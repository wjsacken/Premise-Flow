import os
import requests
import json
import pandas as pd
from datetime import datetime
import re
import logging
import time

# Set up logging
logging.basicConfig(level=logging.INFO)

# Fetch HubSpot Access Token from environment variable
HUBSPOT_ACCESS_TOKEN = os.getenv('HUBSPOT_ACCESS_TOKEN')

if not HUBSPOT_ACCESS_TOKEN:
    raise Exception("HUBSPOT_ACCESS_TOKEN environment variable is not set")

# Headers for HubSpot API requests with Bearer token
HUBSPOT_HEADERS = {
    "Authorization": f"Bearer {HUBSPOT_ACCESS_TOKEN}",
    "Content-Type": "application/json"
}

# Headers for HubSpot API requests with Bearer token
HUBSPOT_HEADERS = {
    "Authorization": f"Bearer {HUBSPOT_ACCESS_TOKEN}",
    "Content-Type": "application/json"
}

# Load enriched data from JSON file
def load_enriched_data(filename="enriched_premises_data.json"):
    with open(filename, 'r') as json_file:
        return json.load(json_file)

# Load sales rep data from CSV file
def load_sales_rep_data(filename="id.csv"):
    return pd.read_csv(filename)

# Load ticket types data from JSON file
def load_ticket_types(filename="ticket_types.json"):
    with open(filename, 'r') as json_file:
        return json.load(json_file)

# Helper function to format dates to YYYY-MM-DD
def format_date(date_str):
    if date_str:
        try:
            # Convert date string to datetime object and format to YYYY-MM-DD
            return datetime.fromisoformat(date_str).strftime('%Y-%m-%d')
        except ValueError:
            return None  # If date format is invalid, return None
    return None

# Helper function to convert date to Unix timestamp (milliseconds)
def format_date_to_timestamp(date_str):
    if date_str:
        try:
            # Convert date string to datetime object and get Unix timestamp in milliseconds
            return int(datetime.fromisoformat(date_str).timestamp() * 1000)
        except ValueError:
            return None  # If date format is invalid, return None
    return None

# Define installation and service pipeline stages
installation_pipeline_stages = {
    "Rejection": 2,
    "closed - rejection - duplication": 2,
    "Closed - rejection - duplication": 2,
    "closed - rejected": 2,
    "Fiber Ready": 3,
    "Active Refusal": 4,
    "Passive Refusal": 258799956,
    "Pre Order": 258799957,
    "New Order": 258799958,
    "NID Relocate": 258799960,
    "Civil Drop": 258799961,
    "civil drop": 258799961,
    "Optical Drop": 258799962,
    "Soft Blockage": 258799963,
    "Hard Blockage": 258799964,
    "NCCH": 258799965,
    "Full Handover": 258799966,
    "NID Installation Complete": 258799967,
    "ISP Scheduled": 258799968,
    "ISP Complete": 258799969,
    "Pending Auto Configuration": 258799970,
    "pending configuration": 258799970,
    "Auto Configuration Failed": 258799971,
    "Activation Complete": 258799972,
    "activation complete": 258799972,
    "Not Actionable": 258799973,
    "Installation": 258799974,
    "Provisioning": 267644843,
    "provisioning failed": 267644843,
    "Provisioned": 267644843,
    "Other": 267644850,
    "NID Installation": 267644851,
    "closed - nid - installation complete": 267644851,
    "Service Activation (without installation)": 267644856,
    "L3 Configuration": 267644930,
    "configured": 267644930,
    "Relocation": 267644931,
    "Abandoned": 954945896
}

service_pipeline_stages = {
    "Cancellation": 267644932,
    "cancelled": 267644932,
    "Cancelled": 267644932,
    "Change Service": 267644933,
    "Service change": 267644933,
    "Change Service": 267644933,
    "Fiber Break": 267644934,
    "Service Down": 267644935,
    "Light Levels": 267647763,
    "Power Down": 267647764,
    "Maintenance": 267647765,
    "Swapout Device": 267647766,
    "Recover Device": 267647767,
    "Deprovisioning": 267647768,
    "Speed Test": 267647769,
    "Change Service Provider": 267647770,
    "Fault": 267647771,
    "service change approved": 954945906,
    "rejected": 955026021,
    "deprovisioned": 954733986
}

# Create or update a contact in HubSpot and return the contact ID
def create_or_update_contact_in_hubspot(premise, customer, sales_rep_data):
    if not premise or not customer:
        logging.warning("Premise or customer data is None, skipping this premise.")
        return
    
    # Extract updated_at from the nested structure
    services = premise.get('services', [])
    service_status_date = None  # Default to None if no date found

    for service in services:
        # Ensure service_details is not None
        service_details = service.get('service_details', None)
        if service_details is None:
            logging.warning(f"Missing service_details for premise {premise.get('id', 'Unknown ID')}. Skipping.")
            continue
        
        full_service = service_details.get('full_service', {})
        service_metadata = full_service.get('service', {})
        
        # Get the updated_at field if it exists
        updated_at = service_metadata.get('updated_at')
        if updated_at:
            # Convert to Unix timestamp in milliseconds
            service_status_date = format_date_to_unix(updated_at)
            break  # Use the first valid updated_at found

    # Prepare contact data
    contact_data = {
        "properties": {
            "firstname": customer.get('first_name', ''),
            "lastname": customer.get('last_name', ''),
            "email": customer.get('email', ''),
            "phone": customer.get('mobile_number', ''),
            "address": f"{premise.get('street_number', '')} {premise.get('street_name', '')}",
            "city": premise.get('city', ''),
            "state": premise.get('province', ''),
            "zip": premise.get('postal_code', ''),
            "aex_id": premise.get('id', ''),
            "latitude": premise.get('latitude', ''),
            "longitude": premise.get('longitude', ''),
            "service_status_date": service_status_date  # Add the Unix timestamp
        }
    }
    
    email = customer.get('email', '')
    aex_id = premise.get('id', '')
    existing_contact_id = find_existing_contact_by_email_or_aex_id(email, aex_id)

    if existing_contact_id:
        # Update the existing contact
        update_contact(existing_contact_id, contact_data)
        return existing_contact_id
    else:
        # Create a new contact
        url = "https://api.hubapi.com/crm/v3/objects/contacts"
        response = requests.post(url, headers=HUBSPOT_HEADERS, json=contact_data)
        
        if response.status_code in (200, 201):
            logging.info(f"Contact created successfully for AEX ID: {aex_id}")
            return response.json().get('id')
        else:
            logging.error(f"Error creating contact: {response.text}")
            return None


    # Extract sales_rep_id and look up sales_rep from the JSON data
    customer_services_items = premise.get('customer', {}).get('customer_services', {}).get('items', [])
    customer_service = customer_services_items[0] if customer_services_items else {}
    if not customer_service:
        logging.debug(f"customer_services_items is empty or not properly structured: {json.dumps(premise.get('customer', {}).get('customer_services', {}), indent=2)}")
    sales_rep_id = customer_service.get('sales_channel_id')
    if sales_rep_id is None:
        logging.debug(f"sales_channel_id not found in customer_service: {json.dumps(customer_service, indent=2)}")
    else:
        logging.debug(f"Extracted sales_rep_id: {sales_rep_id}")

    sales_rep = sales_rep_data.loc[sales_rep_data['sales_channel_id'] == sales_rep_id, 'Sales_Channel_Text'] if pd.notna(sales_rep_id) else pd.Series()
    sales_rep = sales_rep.iloc[0] if not sales_rep.empty else ''

    # Get work_order_id from work_order
    work_order_id = work_order.get('id', '') if work_order else ''

    # Prepare the payload for HubSpot
    contact_data = {
        "properties": {
            "firstname": customer.get('first_name', ''),
            "lastname": customer.get('last_name', ''),
            "email": customer.get('email', ''),
            "phone": customer.get('mobile_number', ''),
            "address": f"{premise.get('street_number', '')} {premise.get('street_name', '')}",
            "city": premise.get('city', ''),
            "state": premise.get('province', ''),
            "zip": premise.get('postal_code', ''),
            "aex_id": premise.get('id', ''),
            "latitude": premise.get('latitude', ''),
            "longitude": premise.get('longitude', ''),
            "work_order_type": work_order.get('type', '') if work_order else '',
            "work_order_last_comment": work_order.get('last_comment', '') if work_order else '',
            "work_order_status": work_order.get('status', '') if work_order else '',
            "work_order_last_update": format_date(work_order.get('updated_at', '')) if work_order else '',
            "work_order_created": format_date(work_order.get('created_at', '')) if work_order else '',
            "work_order_schedule_date": format_date(work_order.get('schedule_date', '')) if work_order else '',
            "work_order_completed_date": format_date(work_order.get('completed_date', '')) if work_order else '',
            "premise_id": premise.get('id', ''),
            "customer_id": customer.get('id', ''),
            "on_network": service.get('on_network', '') if service else '',
            "on_network_date": format_date(service.get('on_network_date', '')) if service else '',
            "completed": service.get('completed', '') if service else '',
            "completed_date": format_date(service.get('completed_date', '')) if service else '',
            "cancelled": service.get('cancelled', '') if service else '',
            "service_cancelled_date": format_date(service.get('cancelled_date', '')) if service else '',
            "sales_rep": sales_rep,
            "sales_rep_id": sales_rep_id,
            "product": product,
            "product_id": productID,
            "service_id": service.get('id', '') if service else '',
            "service_status": service.get('status', '') if service else ''  # Ensure `service_status` is a string value
        }
    }


    # Log the contact data
    logging.info(f"Contact Data: {json.dumps(contact_data, indent=2)}")

    # Create a unique identifier to search for existing contacts by email or AEX ID
    email = customer.get('email', '')
    aex_id = premise.get('id', '')
    existing_contact_id = find_existing_contact_by_email_or_aex_id(email, aex_id)

    if existing_contact_id:
        # Update the existing contact
        update_contact(existing_contact_id, contact_data)
        create_or_update_tickets_for_contact(existing_contact_id, work_order, ticket_types, premise, customer, service, sales_rep_data)
    else:
        # Create a new contact
        url = "https://api.hubapi.com/crm/v3/objects/contacts"
        response = requests.post(url, headers=HUBSPOT_HEADERS, json=contact_data)
        
        if response.status_code in (200, 201):
            logging.info(f"Contact created successfully for AEX ID: {aex_id}")
            contact_id = response.json().get('id')
            if contact_id:
                create_or_update_tickets_for_contact(contact_id, work_order, ticket_types, premise, customer, service, sales_rep_data)
        elif response.status_code == 409:  # Conflict: Contact already exists
            existing_contact_id = extract_existing_contact_id(response.text)
            if existing_contact_id:
                logging.info(f"Conflict detected. Updating existing contact with ID: {existing_contact_id}")
                update_contact(existing_contact_id, contact_data)
                create_or_update_tickets_for_contact(existing_contact_id, work_order, ticket_types, premise, customer, service, sales_rep_data)
        else:
            logging.error(f"Error creating contact: {response.text}")

# Helper function to format dates to YYYY-MM-DD
def format_date(date_str):
    if date_str:
        try:
            # Convert date string to datetime object and format to YYYY-MM-DD
            return datetime.fromisoformat(date_str).strftime('%Y-%m-%d')
        except ValueError:
            return None  # If date format is invalid, return None
    return None

# Helper function to convert date to Unix timestamp (milliseconds)
def format_date_to_unix(date_str, in_milliseconds=True):
    if date_str:
        try:
            # Parse the date string with timezone info
            dt = datetime.fromisoformat(date_str)
            # Convert to Unix timestamp
            unix_timestamp = dt.timestamp()
            # Convert to milliseconds if required
            return int(unix_timestamp * 1000) if in_milliseconds else int(unix_timestamp)
        except ValueError:
            logging.error(f"Invalid date format: {date_str}")
            return None  # Return None for invalid date formats
    return None

# Update an existing contact by ID
def update_contact(contact_id, contact_data):
    url = f"https://api.hubapi.com/crm/v3/objects/contacts/{contact_id}"
    response = requests.patch(url, headers=HUBSPOT_HEADERS, json=contact_data)

    if response.status_code == 200:
        logging.info(f"Contact {contact_id} updated successfully.")
    else:
        logging.error(f"Error updating contact {contact_id}: {response.text}")
        if response.status_code == 409:  # Conflict: Contact already exists
            existing_contact_id = extract_existing_contact_id(response.text)
            if existing_contact_id and existing_contact_id != contact_id:
                logging.info(f"Conflict detected. Retrying update with existing contact ID: {existing_contact_id}")
                update_contact(existing_contact_id, contact_data)

# Extract the existing contact ID from the conflict error message
def extract_existing_contact_id(error_message):
    match = re.search(r"Existing ID: (\d+)", error_message)
    if match:
        return match.group(1)
    return None

# Search for an existing contact by email or AEX ID
def find_existing_contact_by_email_or_aex_id(email, aex_id):
    url = "https://api.hubapi.com/crm/v3/objects/contacts/search"
    query = {
        "filterGroups": [
            {
                "filters": [
                    {
                        "propertyName": "email",
                        "operator": "EQ",
                        "value": email
                    }
                ]
            },
            {
                "filters": [
                    {
                        "propertyName": "aex_id",
                        "operator": "EQ",
                        "value": aex_id
                    }
                ]
            }
        ]
    }
    
    response = requests.post(url, headers=HUBSPOT_HEADERS, json=query)
    
    if response.status_code == 200:
        try:
            data = response.json()
            if data.get('results'):
                return data['results'][0].get('id')  # Return the existing contact ID
        except ValueError:
            logging.error(f"Invalid JSON response: {response.text}")
    else:
        logging.error(f"Error finding contact in HubSpot by email or AEX ID: {response.text}")
    return None

# Create or update tickets in HubSpot for a contact
def create_or_update_tickets_for_contact(contact_id, work_order, ticket_types, premise, customer, service, sales_rep_data):
    if not work_order:
        logging.warning("Work order data is None, skipping ticket creation.")
        return

    # Extract necessary data
    product = (premise.get('services', [{}])[0]
               .get('service_details', {})
               .get('full_service', {})
               .get('isp_product', {})
               .get('name', ''))
    
    customer_services_items = premise.get('customer', {}).get('customer_services', {}).get('items', [])
    sales_rep_id = customer_services_items[0].get('sales_channel_id') if customer_services_items else None

    # Gracefully handle sales rep lookup
    if pd.notna(sales_rep_id):
        matching_rows = sales_rep_data.loc[sales_rep_data['sales_channel_id'] == sales_rep_id, 'Sales_Channel_Text']
        sales_rep = matching_rows.iloc[0] if not matching_rows.empty else 'No Sales Agent Selected'
    else:
        sales_rep = 'No Sales Agent Selected'

    # Extract work order and premise information
    work_order_id = work_order.get('id', '')
    service_id = work_order.get('service_id', '')  # Extract service_id directly from work_order
    premise_id = premise.get('id', '')
    work_order_status = work_order.get('status', '').strip().lower()
    subject = f"{premise.get('street_number', '')} {premise.get('street_name', '')} - {work_order_status}"

    # Define mappings for pipeline and stage
    pipeline_id, pipeline_stage_id = None, None
    lower_case_pipeline_stages = {k.lower(): v for k, v in installation_pipeline_stages.items()}
    if work_order_status in lower_case_pipeline_stages:
        pipeline_id = "0"
        pipeline_stage_id = lower_case_pipeline_stages[work_order_status]
    elif work_order_status == "cancelled":
        logging.info(f"Work order with ID {work_order_id} is 'Cancelled'. No ticket will be created.")
        return  # Skip processing for cancelled work orders
    else:
        logging.error(f"Unknown work order status: '{work_order_status}'. Skipping ticket creation.")
        return

    # Check for existing ticket
    existing_ticket_id = find_existing_ticket_by_work_order_id(work_order_id)

    # Prepare ticket data
    ticket_data = {
        "properties": {
            "subject": subject,
            "content": work_order.get('description', ''),
            "hs_pipeline": pipeline_id,
            "hs_pipeline_stage": pipeline_stage_id,
            "aex_work_order_id": work_order_id,
            "work_order_id1": work_order_id,
            "hubspot_owner_id": None,
            "premise_id": premise_id,
            "customer_id": customer.get('id', ''),
            "createdate": format_date_to_timestamp(work_order.get('created_at', '')),
            "aex_create_date": format_date_to_timestamp(work_order.get('created_at', '')),
            "sales_rep": sales_rep,
            "sales_rep_id": sales_rep_id,
            "schedule_date": format_date_to_timestamp(work_order.get('schedule_date', '')),
            "closed_date": format_date_to_timestamp(work_order.get('completed_date', '')),
            "service_id": service_id,  # Pass extracted service_id here
            "product": product,
            "original_order": work_order_status
        },
        "associations": [
            {
                "to": {
                    "id": contact_id
                },
                "types": [
                    {
                        "associationCategory": "USER_DEFINED",
                        "associationTypeId": 81  # Ticket-to-contact association type ID
                    }
                ]
            }
        ]
    }

    # Create or update ticket
    if existing_ticket_id:
        logging.info(f"Ticket already exists for work order {work_order_id}. Updating existing ticket.")
        try:
            update_ticket(existing_ticket_id, work_order, premise, customer, service, sales_rep_data)
        except Exception as e:
            logging.error(f"Error updating ticket {existing_ticket_id} for work order {work_order_id}: {e}")
    else:
        try:
            url = "https://api.hubapi.com/crm/v3/objects/tickets"
            response = requests.post(url, headers=HUBSPOT_HEADERS, json=ticket_data)

            if response.status_code in (200, 201):
                logging.info(f"Ticket created successfully for work order {work_order_id} and contact {contact_id}")
            else:
                logging.error(f"Error creating ticket for work order {work_order_id}: {response.text}")
        except Exception as e:
            logging.error(f"Error creating ticket for work order {work_order_id}: {e}")

def find_existing_ticket_by_work_order_id(work_order_id):
    """Checks if a ticket with the given `aex_work_order_id` already exists."""
    url = f"https://api.hubapi.com/crm/v3/objects/tickets/search"
    search_data = {
        "filterGroups": [
            {
                "filters": [
                    {
                        "propertyName": "work_order_id1",
                        "operator": "EQ",
                        "value": work_order_id
                    }
                ]
            }
        ],
        "properties": ["hs_object_id"]
    }
    response = requests.post(url, headers=HUBSPOT_HEADERS, json=search_data)

    if response.status_code == 200:
        data = response.json()
        if data.get("total", 0) > 0:
            return data["results"][0]["id"]
    return None

# Update an existing ticket by ID
def update_ticket(ticket_id, work_order, premise, customer, service, sales_rep_data):
    url = f"https://api.hubapi.com/crm/v3/objects/tickets/{ticket_id}"
    if not work_order:
        logging.warning("Work order data is None, skipping ticket creation.")
        return

    # Extract necessary data
    product = (premise.get('services', [{}])[0]
               .get('service_details', {})
               .get('full_service', {})
               .get('isp_product', {})
               .get('name', ''))
    
    customer_services_items = premise.get('customer', {}).get('customer_services', {}).get('items', [])
    sales_rep_id = customer_services_items[0].get('sales_channel_id') if customer_services_items else None

    # Gracefully handle sales rep lookup
    if pd.notna(sales_rep_id):
        matching_rows = sales_rep_data.loc[sales_rep_data['sales_channel_id'] == sales_rep_id, 'Sales_Channel_Text']
        sales_rep = matching_rows.iloc[0] if not matching_rows.empty else 'No Sales Agent Selected'
    else:
        sales_rep = 'No Sales Agent Selected'

    # Extract work order and premise information
    work_order_id = work_order.get('id', '')
    service_id = work_order.get('service_id', '')  # Extract service_id directly from work_order
    premise_id = premise.get('id', '')
    work_order_status = work_order.get('status', '').strip().lower()
    subject = f"{premise.get('street_number', '')} {premise.get('street_name', '')} - {work_order_status}"

    # Define mappings for pipeline and stage
    pipeline_id, pipeline_stage_id = None, None
    lower_case_pipeline_stages = {k.lower(): v for k, v in installation_pipeline_stages.items()}
    if work_order_status in lower_case_pipeline_stages:
        pipeline_id = "0"
        pipeline_stage_id = lower_case_pipeline_stages[work_order_status]
    elif work_order_status == "cancelled":
        logging.info(f"Work order with ID {work_order_id} is 'Cancelled'. No ticket will be created.")
        return  # Skip processing for cancelled work orders
    else:
        logging.error(f"Unknown work order status: '{work_order_status}'. Skipping ticket creation.")
        return

    # Check for existing ticket
    existing_ticket_id = find_existing_ticket_by_work_order_id(work_order_id)

    # Prepare ticket data
    ticket_data = {
        "properties": {
            "subject": subject,
            "content": work_order.get('description', ''),
            "hs_pipeline": pipeline_id,
            "hs_pipeline_stage": pipeline_stage_id,
            "aex_work_order_id": work_order_id,
            "work_order_id1": work_order_id,
            "hubspot_owner_id": None,
            "premise_id": premise_id,
            "customer_id": customer.get('id', ''),
            "createdate": format_date_to_timestamp(work_order.get('created_at', '')),
            "aex_create_date": format_date_to_timestamp(work_order.get('created_at', '')),
            "sales_rep": sales_rep,
            "sales_rep_id": sales_rep_id,
            "schedule_date": format_date_to_timestamp(work_order.get('schedule_date', '')),
            "closed_date": format_date_to_timestamp(work_order.get('completed_date', '')),
            "service_id": service_id,  # Pass extracted service_id here
            "product": product,
        }
    }

    # Log the ticket data being sent
    logging.info(f"Updating Ticket Data: {json.dumps(ticket_data, indent=2)}")
    
    response = requests.patch(url, headers=HUBSPOT_HEADERS, json=ticket_data)

    if response.status_code == 200:
        logging.info(f"Ticket {ticket_id} updated successfully.")
    else:
        logging.error(f"Error updating ticket {ticket_id}: {response.text}")

# Search for an existing ticket by work_order_id, premise_id, and contact_id
def find_existing_ticket_by_work_order_and_contact(work_order_id, premise_id, contact_id):
    url = "https://api.hubapi.com/crm/v3/objects/tickets/search"
    query = {
        "filterGroups": [
            {
                "filters": [
                    {
                        "propertyName": "work_order_id1",  # Ensure this matches the exact custom property name in HubSpot
                        "operator": "EQ",
                        "value": work_order_id
                    }
                ]
            },
            {
                "filters": [
                    {
                        "propertyName": "premise_id",
                        "operator": "EQ",
                        "value": premise_id
                    },
                    {
                        "propertyName": "associations.contact",
                        "operator": "EQ",
                        "value": contact_id
                    }
                ]
            }
        ]
    }

    logging.info(f"Searching for existing ticket with work_order_id: {work_order_id}, premise_id: {premise_id}, contact_id: {contact_id}")
    response = requests.post(url, headers=HUBSPOT_HEADERS, json=query)

    if response.status_code == 200:
        try:
            data = response.json()
            logging.info(f"Search response data: {json.dumps(data, indent=2)}")  # Log the response data for debugging
            if data.get('results'):
                ticket_id = data['results'][0].get('id')
                logging.info(f"Found existing ticket with ID: {ticket_id} for work order ID: {work_order_id}")
                return ticket_id  # Return the existing ticket ID
        except ValueError:
            logging.error(f"Invalid JSON response: {response.text}")
    else:
        logging.error(f"Error finding ticket in HubSpot by work_order_id, premise ID, and contact ID: {response.text}")

    logging.info("No existing ticket found. Proceeding with ticket creation.")
    return None

# Process premises data and create or update contacts and tickets in HubSpot for multiple work orders
def process_premises_for_hubspot():
    premises_data = load_enriched_data()
    sales_rep_data = load_sales_rep_data()
    ticket_types = load_ticket_types()

    for premise in premises_data:
        if not premise:
            logging.warning("Premise data is None, skipping this premise.")
            continue

        logging.debug(f"Processing premise: {json.dumps(premise, indent=2)}")

        customer = premise.get('customer')
        if customer is None:
            logging.warning("Customer data is missing, skipping this premise.")
            continue

        customer_details = customer.get('customer_details', {})
        if not customer_details:
            logging.warning("Customer details are missing, skipping this premise.")
            continue

        customer_services_items = customer.get('customer_services', {}).get('items', [])
        service_id = customer_services_items[0].get('id') if customer_services_items else None

        if not service_id:
            logging.warning("Service ID is missing, skipping this premise.")
            continue

        services = premise.get('services', [])
        if not isinstance(services, list):
            logging.error(f"Expected 'services' to be a list, but got {type(services)}. Skipping premise.")
            continue

        logging.debug(f"Services for premise: {json.dumps(services, indent=2)}")

        contact_id = create_or_update_contact_in_hubspot(premise, customer_details, sales_rep_data)

        if contact_id:
            for service in services:
                if not isinstance(service, dict):
                    logging.warning(f"Invalid service object: {service}. Skipping.")
                    continue

                logging.debug(f"Processing service: {json.dumps(service, indent=2)}")

                service_details = service.get('service_details')
                if not service_details or not isinstance(service_details, dict):
                    logging.warning("Service details are missing or invalid, skipping service.")
                    continue

                full_service = service_details.get('full_service', {})
                if not isinstance(full_service, dict):
                    logging.warning("Full service details are missing or invalid, skipping service.")
                    continue

                logging.debug(f"Processing full_service: {json.dumps(full_service, indent=2)}")

                work_orders_data = service.get('work_orders')
                if not work_orders_data or not isinstance(work_orders_data, dict):
                    logging.warning("Work orders data is missing or invalid, skipping service.")
                    continue

                work_orders = work_orders_data.get('items', [])
                if not isinstance(work_orders, list):
                    logging.warning(f"Expected 'work_orders' to be a list, but got {type(work_orders)}. Skipping service.")
                    continue

                logging.debug(f"Work orders for service: {json.dumps(work_orders, indent=2)}")

                for work_order in work_orders:
                    if not isinstance(work_order, dict):
                        logging.warning(f"Invalid work order object: {work_order}. Skipping.")
                        continue

                    logging.debug(f"Processing work order: {json.dumps(work_order, indent=2)}")

                    # Validate ticket creation inputs before proceeding
                    if not contact_id or not ticket_types:
                        logging.error("Required data for ticket creation is missing, skipping work order.")
                        continue

                    try:
                        create_or_update_tickets_for_contact(
                            contact_id,
                            work_order,
                            ticket_types,
                            premise,
                            customer_details,
                            {"id": service_id},
                            sales_rep_data
                        )
                    except Exception as e:
                        logging.error(f"Error creating or updating tickets: {e}")

# Run the main function
if __name__ == "__main__":
    process_premises_for_hubspot()
