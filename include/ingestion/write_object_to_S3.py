import boto3 
import json 

def write_to_s3(object_to_store, base_filename, minio_url, minio_access_key, minio_secret_key, current_timestamp): 
    s3 = boto3.client( 
        "s3",
        endpoint_url = minio_url,
        aws_access_key_id = minio_access_key,
        aws_secret_access_key = minio_secret_key
    ) 

    curren_year = current_timestamp.year 
    current_month = current_timestamp.month
    current_day = current_timestamp.day
    current_hour = current_timestamp.hour 

    s3object = s3.Object('bronze', f'{base_filename}/{curren_year}/{current_month}/{current_day}/{current_hour}')
    object_body = {
        'data': object_to_store
    }
    s3object.put(
            Body=(bytes(json.dumps(object_body).encode('UTF-8'))),
            ContentType="application/json"
        )
    
    