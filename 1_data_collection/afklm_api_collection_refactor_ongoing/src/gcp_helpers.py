from io import BytesIO
import pandas as pd
import os
import re

def read_csv(path_folder: str, path_file: str, bucket=None, on_cloud=False):
    import pandas as pd
    from io import BytesIO

    if on_cloud and bucket:
        blob = bucket.blob(path_file)
        csv_data = blob.download_as_bytes()
        return pd.read_csv(BytesIO(csv_data), low_memory=False)
    else:
        return pd.read_csv(os.path.join(path_folder, path_file), low_memory=False)

def save_csv(df, path_folder: str, path_file: str, bucket=None, on_cloud=False):
    if on_cloud and bucket:
        blob = bucket.blob(path_file)
        from io import BytesIO
        buffer = BytesIO(bytes(df.to_csv(index=False), encoding='utf-8'))
        blob.upload_from_string(buffer.getvalue(), content_type="text/csv")
    else:
        df.to_csv(os.path.join(path_folder, path_file), index=False)


def list_files(file_folder: str, pattern: str = '.', ext: str = '.', bucket=None, on_cloud=False) -> list:
    """
    List files in a folder (local or GCS bucket) filtered by pattern and extension.

    Args:
        file_folder (str): Local folder path or GCS prefix
        pattern (str): Regex pattern to search in filenames
        ext (str): File extension to filter (e.g., 'csv', 'json')
        bucket: GCS bucket object (if on_cloud=True)
        on_cloud (bool): Whether to list files on GCP bucket or locally

    Returns:
        list: Sorted list of matching filenames
    """
    file_list = []

    if on_cloud and bucket:
        # List GCS blobs under the given prefix
        blobs = bucket.list_blobs(prefix=file_folder)
        for blob in blobs:
            name = blob.name
            if (pattern in name) and re.search(f"{ext}$", name):
                file_list.append(name)
    else:
        # List local files
        for file in os.listdir(file_folder):
            if (pattern in file) and re.search(f"{ext}$", file):
                file_list.append(file)

    file_list.sort()
    return file_list
