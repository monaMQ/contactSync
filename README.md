# Kafka Producer and Consumer with FastAPI and Salesforce Integration

This project consists of two main components: a Kafka Producer implemented using FastAPI, and a Kafka Consumer that interacts with Salesforce. 
The Producer sends contact data to a Kafka topic, while the Consumer processes these messages and either creates or updates contact records in Salesforce.

## Prerequisites

### Saleforce OAuth
- Create config.toml in put it in consumer folder

#### Sample `config.toml`

```toml
[salesforce]
client_id = "your_client_id"
client_secret = "your_client_secret"
username = "your_username"
password = "your_password"
auth_url = "https://login.salesforce.com/services/oauth2/token"
api_base_url = "https://your_instance.salesforce.com/services/data/vXX.X/sobjects/Contact"
```

#### How to use

1. run `docker-compose up` on terminal
2. send contact data through Postman (without Mulesoft Flex Gateway): 

Post method: 
`http://localhost:8000/process_contact`
Body
`[
    {
        "action": "create",
        "first_name": "Ava",
        "last_name": "Rodriguez",
        "email": "ava.rodriguez@example.com",
        "phone": "+61-4987-234-567",
        "address": "456 Oak St, Miami, FL"
    },
    {
        "action": "update",
        "contact_id": "0038r00000ROfyXAAT",
        "first_name": "Emily",
        "address": "415 Mission Street San Francisco CA 94105 United States"
    }
]`

3. You can monitor the messages produced by the Producer using Kafdrop by visting Kafdrop UI at `localhost:9000`


### API Endpoints (Producer) Details:
__POST /process_contact__
Sends a list of contact data to the contact_events Kafka topic.
Request Body: A JSON array of contact objects.
Response: A success message with the count of contacts sent, or an error message if the operation fails.

__GET /hello__
Sends a "Hello, World!" message to the topic_hello Kafka topic.
Response: A JSON object indicating success on the console.

### Consumer Details:
__Kafka Consumer:__
The Consumer script subscribes to the contact_events Kafka topic.
It processes messages based on the action field:
    - create: Inserts a new contact into Salesforce.
    - update: Updates an existing contact in Salesforce using the contact_id.

__Salesforce Integration:__
The Consumer interacts with Salesforce using OAuth 2.0 for authentication.
It uses the Salesforce REST API to create or update contact records.
The Consumer logs its activity, including successful Salesforce API calls and any errors encountered during processing.

### Logging
Both the Producer and Consumer applications log their activities to the console, 
including details about each message produced or consumed, and any errors encountered.

### Troubleshooting
- Kafka Issues: Ensure that Kafka is up and running before starting the Producer or Consumer. If the Producer or Consumer fails to connect to Kafka, it will log an error message.
- Salesforce API Errors (Consumer): The Consumer will log any issues related to Salesforce API requests.
- Configuration Issues (Consumer): Verify that the config.toml file is correctly set up with valid Salesforce credentials.