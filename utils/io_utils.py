import os
import requests

def download_file(url, dest, headers=None):
    """Download a file from a URL to a destination path, with optional headers."""
    if os.path.exists(dest):
        print(f"{dest} already exists. Skipping download.")
        return
    print(f"Downloading from {url} ...")
    r = requests.get(url, headers=headers)
    if r.status_code != 200:
        raise Exception(f"Failed to download {url}. Status code: {r.status_code}")
    with open(dest, "wb") as f:
        f.write(r.content)
    print(f"Downloaded to {dest}") 