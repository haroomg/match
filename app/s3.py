from dotenv import load_dotenv
import boto3
import os

# Obtenemos los env
load_dotenv("../.env")

# Crea una instancia del cliente de S3
s3 = boto3.client(
    's3',
    aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY")
)

def get_buckets() -> list:
    
    return [bucket["Name"] for bucket in s3.list_buckets()["Buckets"]]