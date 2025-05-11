"""Google Cloud Storage Service for handling file uploads."""
import logging
from google.cloud import storage
import os
from werkzeug.utils import secure_filename
from datetime import datetime

class GoogleCloudStorageService:
    def __init__(self):
        """Initialize the Google Cloud Storage client."""
        try:
            self.client = storage.Client()
            self.bucket_name = os.getenv('GCS_BUCKET_NAME')
            self.bucket = self.client.bucket(self.bucket_name)
        except Exception as e:
            logging.error(f"Error initializing Google Cloud Storage: {str(e)}")
            raise

    def upload_file(self, file, filename):
        """
        Upload a file to Google Cloud Storage.
        
        Args:
            file: The file object to upload
            filename: The name to give the file in storage
            
        Returns:
            The public URL of the uploaded file
        """
        try:
            # Generate a unique filename with timestamp if needed
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            unique_filename = f"{os.path.splitext(filename)[0]}_{timestamp}{os.path.splitext(filename)[1]}"
            
            # Create a blob and upload the file
            blob = self.bucket.blob(unique_filename)
            
            # Upload the file
            file.seek(0)  # Make sure to read from the beginning of the file
            blob.upload_from_file(file, content_type=file.content_type)
            
            # Generate the public URL - don't try to set individual ACLs
            public_url = f"https://storage.googleapis.com/{self.bucket_name}/{unique_filename}"
            
            logging.info(f"File uploaded successfully to {public_url}")
            return public_url
            
        except Exception as e:
            logging.error(f"Error uploading file to Google Cloud Storage: {str(e)}")
            raise