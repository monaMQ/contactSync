from confluent_kafka import Consumer, KafkaError, TopicPartition
import time
import logging
import json
import requests
import toml

# Kafka Consumer configuration
conf = {
    'bootstrap.servers': 'kafka:29092',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True,
    'group.id': 'my-group',
    'api.version.request': True,
    'api.version.fallback.ms': 0
}

# Load Salesforce OAuth 2.0 Configuration from the TOML file
def load_salesforce_config(config_file='config.toml'):
    """Load Salesforce configuration from a TOML file."""
    try:
        config = toml.load(config_file)
        logging.info("Salesforce configuration loaded successfully")
        return config['salesforce']
    except Exception as e:
        logging.error(f"Error loading Salesforce configuration: {e}")
        raise

# Salesforce OAuth 2.0 Configuration
salesforce_conf = load_salesforce_config()

def get_salesforce_token():
    """Obtain an OAuth 2.0 token from Salesforce."""
    payload = {
        'grant_type': 'password',
        'client_id': salesforce_conf['client_id'],
        'client_secret': salesforce_conf['client_secret'],
        'username': salesforce_conf['username'],
        'password': salesforce_conf['password']
    }

    response = requests.post(salesforce_conf['auth_url'], data=payload)
    
    if response.status_code == 200:
        logging.info("Salesforce OAuth token obtained successfully")
        return response.json().get('access_token')
    else:
        logging.error(f"Failed to obtain Salesforce token: {response.text}")
        raise Exception("Failed to obtain Salesforce token")

def map_contact_fields(contact_data):
    """Map JSON fields to Salesforce API fields."""
    field_mapping = {
        "first_name": "FirstName",
        "last_name": "LastName",
        "email": "Email",
        "phone": "Phone",
        "address": "MailingStreet"
    }
    
    mapped_data = {}
    for key, value in contact_data.items():
        if value is not None:  # Skip null values
            mapped_key = field_mapping.get(key, key)  # Map to Salesforce field names or keep the original key
            mapped_data[mapped_key] = value
    
    return mapped_data

def insert_salesforce_contact(access_token, contact):
    """Insert a Salesforce contact using the Salesforce API."""
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }

    api_base_url = "https://mqu--muletrain.sandbox.my.salesforce.com/services/data/v61.0/sobjects/Contact/"
    
    # Map fields before sending to Salesforce
    mapped_data = map_contact_fields(contact)
    mapped_data.pop('contact_id', None)  # Remove contact_id from data as it's part of the URL
    mapped_data.pop('action', None)  # Remove the action field before sending the data

    logging.debug(f"Sending data to Salesforce: {json.dumps(mapped_data)}")
    response = requests.post(api_base_url, headers=headers, json=mapped_data)

    logging.debug(f"Salesforce response status: {response.status_code}")
    logging.debug(f"Salesforce response body: {response.text}")

    if response.status_code == 201:
        logging.info(f"Salesforce contact inserted successfully")
        return True
    else:
        logging.error(f"Failed to insert Salesforce contact: {response.status_code} - {response.text}")
        return False

def consume_messages():
    consumer = Consumer(conf)
    topic_partition = TopicPartition('contact_events', 1)  # Listen only to partition 1
    consumer.assign([topic_partition])

    try:
        # Obtain Salesforce OAuth token
        access_token = get_salesforce_token()

        while True:
            msg = consumer.poll(1.0)
            logging.info("Consumer Polling")

            if msg is None:
                logging.info("No message received")
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logging.info(f'Reached end of partition: {msg.topic()}[{msg.partition()}]')
                else:
                    logging.error(f'Error while consuming messages: {msg.error()}')
            else:
                message = msg.value().decode('utf-8')
                logging.info(f"Received message: {message}")
                contact_data = json.loads(message)

                # Ensure contact_data is a list for bulk processing
                if isinstance(contact_data, dict):
                    contact_data = [contact_data]  # Wrap single contact in a list
                elif isinstance(contact_data, str):
                    logging.error("Expected a dictionary or list of dictionaries, but got a string.")
                    continue

                for contact in contact_data:
                    insert_salesforce_contact(access_token, contact)

    except Exception as e:
        logging.error(f"Exception occurred while consuming messages: {e}")
    finally:
        consumer.close()
        logging.info("Consumer closed")

def startup():
    logging.info("Starting consumer...")
    time.sleep(30)  # Allow time for Kafka to start up
    consume_messages()

if __name__ == "__main__":
    try:
        startup()
    except Exception as e:
        logging.error(f"Exception occurred: {e}")
