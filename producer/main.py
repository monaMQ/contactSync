from confluent_kafka import Producer
from fastapi import FastAPI
from pydantic import BaseModel
from typing import List

app = FastAPI()

producer_conf = {
    'bootstrap.servers': 'kafka:29092',
    'client.id': 'my-app'
}

producer = Producer(producer_conf)
topic = 'contact_events'

# Define the data model using Pydantic
class Contact(BaseModel):
    action: str
    contact_id: str = None  # Optional for create, required for update
    first_name: str
    last_name: str = None  # Optional
    email: str = None  # Optional
    phone: str = None  # Optional
    address: str = None  # Optional

@app.post("/process_contact")
def process_contact(contacts: List[Contact]):
    try:
        for contact in contacts:
            contact_data = contact.json().encode('utf-8')
            
            # Determine the partition based on the action
            if contact.action == 'create':
                partition = 1
            elif contact.action == 'update':
                partition = 2
            else:
                return {"status": "error", "message": f"Unknown action: {contact.action}"}
            
            # Produce the message to the specified partition
            producer.produce(topic, value=contact_data, partition=partition)
        
        producer.flush()
        return {"status": "success", "message": f"Successfully processed {len(contacts)} contacts"}
    except Exception as e:

        return {"status": "error", "message": str(e)}

@app.get("/hello")
def say_hello():
    return {"message": "Hello, World!"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
