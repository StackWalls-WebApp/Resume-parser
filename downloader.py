# downloader.py
import os
import requests
from urllib.parse import urlparse
from requests.utils import requote_uri
import validators

def download_file_from_url(url, download_folder="Downloads"):
    """Download a file from a URL and save it locally."""
    try:
        # Ensure the URL has a scheme
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url

        # Encode the URL to handle spaces and special characters
        url = requote_uri(url)

        if not validators.url(url):
            raise ValueError("Invalid URL")

        parsed_url = urlparse(url)
        filename = os.path.basename(parsed_url.path)
        if not filename:
            # Generate a default filename if URL does not end with a file
            filename = 'downloaded_resume.pdf'
        os.makedirs(download_folder, exist_ok=True)
        file_path = os.path.join(download_folder, filename)
        
        response = requests.get(url, stream=True, timeout=15)
        response.raise_for_status()

        content_type = response.headers.get('Content-Type', '').lower()
        if 'pdf' not in content_type and 'word' not in content_type and 'octet-stream' not in content_type:
            raise ValueError(f"Invalid Content-Type: {content_type}")
        with open(file_path, 'wb') as file:
            for chunk in response.iter_content(1024):
                file.write(chunk)
        return file_path
    except Exception as e:
        raise Exception(f"Error downloading file: {e}")
