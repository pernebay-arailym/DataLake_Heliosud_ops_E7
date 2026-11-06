import boto3
from botocore.config import Config
import sys

# SeaweedFS S3 Endpoint Configuration
S3_ENDPOINT = "http://localhost:8333"
BUCKETS = ["raw", "staging", "curated", "archive"]

def get_s3_client():
    """Initializes and returns an S3 client configured for SeaweedFS."""
    return boto3.client(
        's3',
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id='any',        # SeaweedFS default unauthenticated mode accepts any key
        aws_secret_access_key='any',
        config=Config(s3={'addressing_style': 'path'}), # Mandatory path-style addressing for local endpoints
        region_name='us-east-1'
    )

def init_buckets():
    s3 = get_s3_client()
    print("--- Initializing HélioSud Data Lake Buckets ---")
    
    # 1. List existing buckets
    existing_response = s3.list_buckets()
    existing_buckets = [b['Name'] for b in existing_response.get('Buckets', [])]
    
    # 2. Create target buckets if they do not exist
    for bucket in BUCKETS:
        if bucket in existing_buckets:
            print(f"[INFO] Bucket '{bucket}' already exists.")
        else:
            try:
                s3.create_bucket(Bucket=bucket)
                print(f"[SUCCESS] Created bucket: '{bucket}'")
            except Exception as e:
                print(f"[ERROR] Failed to create bucket '{bucket}': {e}")
                sys.exit(1)

    # 3. Perform a read/write verification test on the 'raw' bucket
    print("\n--- Running Storage Read/Write Verification Test ---")
    test_key = "healthcheck/test.txt"
    test_content = b"SeaweedFS operational check - Data Lake initialized successfully."
    
    try:
        # Write Test
        s3.put_object(Bucket="raw", Key=test_key, Body=test_content)
        print(f"[SUCCESS] Wrote verification file to 'raw/{test_key}'")
        
        # Read Test
        read_res = s3.get_object(Bucket="raw", Key=test_key)
        retrieved_content = read_res['Body'].read()
        
        if retrieved_content == test_content:
            print(f"[SUCCESS] Read verification succeeded! Content: '{retrieved_content.decode()}'")
        else:
            print("[ERROR] Retrieved content does not match sample data!")
            sys.exit(1)
            
    except Exception as e:
        print(f"[ERROR] Read/Write test failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    init_buckets()
