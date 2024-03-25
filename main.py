import os
from dotenv import load_dotenv
import boto3
from google.cloud import storage
# from google.auth import service_account
from google.oauth2.service_account import Credentials



def create_gcs_client():
    """Creates a Google Cloud Storage client using credentials from a service account key file.

    Returns:
        google.cloud.storage.Client: The GCS client object.
    """

    # Read credentials from the service account key file
    credentials = Credentials.from_service_account_file("google-storage.json")

    # Create GCS client using the loaded credentials
    return storage.Client(credentials=credentials)


def transfer_gcs_to_s3(source_bucket_name, dest_bucket_name):
    """
    Transfers all files from a GCS bucket to an S3 bucket.

    Args:
        source_bucket_name (str): Name of the source bucket in GCS.
        dest_bucket_name (str): Name of the destination bucket in S3.
    """

    # Initialize GCS and S3 clients
    gcs_client = create_gcs_client()
    # Initialize S3 client with credentials from environment variables
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
    )

    # Get all blobs (files) from the source bucket
    bucket = gcs_client.get_bucket(source_bucket_name)
    blobs = bucket.list_blobs()  # This retrieves all blobs (files)

    # Iterate through each blob and transfer to S3
    for blob in blobs:
        source_file_path = blob.name  # Get the file path within GCS bucket
        dest_file_path = f"gcs/{source_file_path}"  # You can customize the destination path here

        # Download data from GCS and upload to S3
        data = blob.download_as_string()
        s3_client.put_object(Body=data, Bucket=dest_bucket_name, Key=dest_file_path)

        print(f"Transferred: {source_file_path} -> {dest_file_path}")


# Example usage
SOURCE_BUCKET_NAME = os.environ.get("SOURCE_BUCKET_NAME")
DESTINATION_BUCKET_NAME = os.environ.get("DESTINATION_BUCKET_NAME")

transfer_gcs_to_s3(SOURCE_BUCKET_NAME, DESTINATION_BUCKET_NAME)
