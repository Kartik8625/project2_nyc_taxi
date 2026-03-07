import boto3
import os
import logging
from pathlib import Path

# ─────────────────────────────────────────
# LOGGING SETUP
# ─────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s — %(levelname)s — %(message)s'
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────
BUCKET_NAME = "kartik-nyc-taxi-pipeline"
LOCAL_PROCESSED = "data/processed/nyc_taxi_clean"
S3_PREFIX = "processed/nyc_taxi_clean"

# ─────────────────────────────────────────
# S3 CLIENT
# ─────────────────────────────────────────
s3_client = boto3.client("s3", region_name="ap-south-1")

def upload_folder_to_s3(local_folder, bucket, s3_prefix):
    """Upload entire folder recursively to S3"""
    
    local_path = Path(local_folder)
    
    if not local_path.exists():
        raise Exception(f"Local folder not found: {local_folder}")
    
    uploaded = 0
    total_size = 0
    
    # Walk through all files recursively
    for file_path in local_path.rglob("*"):
        if file_path.is_file():
            
            # Build S3 key maintaining folder structure
            relative_path = file_path.relative_to(local_path)
            s3_key = f"{s3_prefix}/{relative_path}"
            
            # Get file size
            file_size = os.path.getsize(file_path)
            total_size += file_size
            
            logger.info(f"Uploading: {file_path} → s3://{bucket}/{s3_key}")
            
            # Upload file
            s3_client.upload_file(
                str(file_path),
                bucket,
                s3_key
            )
            uploaded += 1
            logger.info(f"✅ Uploaded ({uploaded}): {s3_key}")
    
    return uploaded, total_size

# ─────────────────────────────────────────
# MAIN UPLOAD
# ─────────────────────────────────────────
print("\n" + "="*55)
print("NYC TAXI — S3 UPLOAD")
print("="*55)

try:
    logger.info(f"Starting upload to s3://{BUCKET_NAME}/{S3_PREFIX}")
    
    count, size = upload_folder_to_s3(
        LOCAL_PROCESSED,
        BUCKET_NAME,
        S3_PREFIX
    )
    
    size_mb = size / (1024 * 1024)
    
    print("\n" + "="*55)
    print("UPLOAD SUMMARY")
    print("="*55)
    print(f"Files uploaded : {count}")
    print(f"Total size     : {size_mb:.2f} MB")
    print(f"S3 location    : s3://{BUCKET_NAME}/{S3_PREFIX}")
    print(f"AWS Region     : ap-south-1 (Mumbai)")
    print("="*55)
    print("✅ DATA SUCCESSFULLY UPLOADED TO AWS S3!")
    print("="*55)

except Exception as e:
    logger.error(f"❌ Upload failed: {e}")
    raise

# ─────────────────────────────────────────
# VERIFY UPLOAD
# ─────────────────────────────────────────
logger.info("Verifying uploaded files on S3...")

response = s3_client.list_objects_v2(
    Bucket=BUCKET_NAME,
    Prefix=S3_PREFIX
)

print("\n" + "="*55)
print("S3 VERIFICATION — FILES ON CLOUD")
print("="*55)

total_s3_size = 0
for obj in response.get("Contents", []):
    size_kb = obj["Size"] / 1024
    total_s3_size += obj["Size"]
    print(f"✅ {obj['Key']} ({size_kb:.1f} KB)")

print(f"\nTotal on S3: {total_s3_size/(1024*1024):.2f} MB")
print("="*55)