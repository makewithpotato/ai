# Run model invocation with InvokeModel
import boto3
import json
import os
from dotenv import load_dotenv

# Create the model-specific input
model_id = "twelvelabs.marengo-embed-2-7-v1:0"
# Replace the us prefix depending on your region
inference_profile_id = "apac.twelvelabs.marengo-embed-2-7-v1:0"

model_input = {
  "inputType": "text",
  "inputText": "man walking a dog"
}

load_dotenv()

# Initialize the Bedrock Runtime client
client = boto3.client(service_name='bedrock-runtime',
        region_name=os.getenv("AWS_DEFAULT_REGION"))

# Make the request
response = client.invoke_model(
    modelId=inference_profile_id,
    body=json.dumps(model_input)
)

# Print the response body
response_body = json.loads(response['body'].read().decode('utf-8'))

print(response_body)