import requests
import boto3
import json

def get_secret(secret_name, region_name='ap-south-1'):
    client = boto3.client('secretsmanager', region_name=region_name)
    response = client.get_secret_value(SecretId=secret_name)
    
    # Secrets Manager returns a string, need to parse JSON
    secret = json.loads(response['SecretString'])
    return secret


def send_slack_webhook(message):
    secret = get_secret("slack_credentials")
    team_id = secret["team_id"]
    channel_id = secret["channel_id"]
    webhook_token = secret["webhook_token"]
    webhook_url = f"https://hooks.slack.com/services/{team_id}/{channel_id}/{webhook_token}"
    payload = {"text": message}
    response = requests.post(webhook_url, json=payload)
    if response.status_code == 200:
        print("✅ Message sent")
    else:
        print(f"❌ Failed: {response.status_code} - {response.text}")
