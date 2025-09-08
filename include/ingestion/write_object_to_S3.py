import boto3 
import json 

def write_to_s3(object_to_store, base_filename, minio_url, minio_access_key, minio_secret_key, current_timestamp): 
    s3 = boto3.client(
        "s3",
        endpoint_url=minio_url,
        aws_access_key_id=minio_access_key,
        aws_secret_access_key=minio_secret_key
    )

    current_year = current_timestamp.year 
    current_month = current_timestamp.month
    current_day = current_timestamp.day
    current_hour = current_timestamp.hour 
    
    object_key = f"{base_filename}/year={current_year}/month={current_month}/day={current_day}/hour={current_hour}.json"
    object_body = {"data": object_to_store}

    s3.put_object(
        Bucket="bronze",
        Key=object_key,
        Body=json.dumps(object_body).encode("utf-8"),
        ContentType="application/json"
    )