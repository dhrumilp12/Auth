import os
from urllib.parse import urlparse
from azure.storage.blob import BlobServiceClient
from werkzeug.utils import secure_filename
from azure.core.exceptions import ResourceExistsError

class AzureBlobService:
    """
    Utility class to handle Azure Blob Storage operations.
    """

    def __init__(self, container_name=None):
        self.connection_string = os.getenv('AZURE_BLOB_CONNECTION_STRING')
        # Use the provided container_name or default to "profile-pics"
        self.container_name = container_name or os.getenv('AZURE_BLOB_CONTAINER_NAME', "profile-pics")

        self.blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
        self.container_client = self.blob_service_client.get_container_client(self.container_name)

        try:
            self.container_client.create_container()
        except ResourceExistsError:
            pass

    def _get_blob_name_from_url(self, blob_url: str) -> str:
        """
        Given a full blob URL, extract the blob name (file name).
        Example: 
          blob_url = "https://<account>.blob.core.windows.net/profile-pics/username_avatar.png"
          returns "username_avatar.png"
        """
        parsed_url = urlparse(blob_url)
        # parsed_url.path might be something like "/container-name/username_avatar.png"
        # The first character could be '/', so we strip it off:
        path_parts = parsed_url.path.lstrip('/').split('/')
        # path_parts[0] should be the container name (e.g. 'profile-pics'),
        # path_parts[1] (and onward) would be the actual blob name.
        # If your container name and blob name are separated, handle accordingly:
        if len(path_parts) > 1:
            # everything except the first item is the blob name (in case there are subdirectories)
            blob_name = '/'.join(path_parts[1:])
        else:
            # If there is no separate container part or something unexpected, fallback:
            blob_name = path_parts[0]
        return blob_name

    def upload_file(self, file_obj, filename: str) -> str:
        """
        Uploads a file-like object to Azure Blob Storage with a safe filename.
        Returns the publicly accessible URL of the uploaded blob (if your container is public).
        """
        secure_name = secure_filename(filename)
        blob_client = self.container_client.get_blob_client(blob=secure_name)
        
        # Upload the file (file_obj is a file-like, e.g., request.files['profile_picture'])
        blob_client.upload_blob(file_obj, overwrite=True)
        
        # Construct a URL. If your container is not public, you'll need a SAS token or other approach.
        blob_url = blob_client.url
        return blob_url

    def delete_blob(self, blob_url: str):
        """
        Deletes a blob given its full URL, if it exists.
        """
        if not blob_url:
            return  # Nothing to delete

        blob_name = self._get_blob_name_from_url(blob_url)
        blob_client = self.container_client.get_blob_client(blob=blob_name)
        
        # This will succeed silently if the blob does not exist
        blob_client.delete_blob()
    
    @staticmethod
    def get_group_images_service():
        """
        Returns an instance of AzureBlobService configured for group images.
        """
        return AzureBlobService(container_name="group-images")