import os, json, gzip
from colorama import Fore

def info_message(text: str, color: str = None) -> None:
    """Print colored info messages"""
    if color is None:
        print(Fore.RESET + text)
    else:
        print(eval(f'Fore.{color.upper()}') + text)

def compress_json(data, path: str, bucket=None, on_cloud=False):
    import gzip, json
    from io import BytesIO

    if on_cloud and bucket:
        buffer = BytesIO()
        with gzip.GzipFile(fileobj=buffer, mode='wb') as f:
            f.write(json.dumps(data, ensure_ascii=False, indent=4).encode("utf-8"))
        blob = bucket.blob(path)
        blob.upload_from_file(BytesIO(buffer.getvalue()))
    else:
        with gzip.open(path, 'wt', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

def read_compressed_json(path: str):
    """Read compressed JSON from .gz"""
    with gzip.open(path) as f:
        return json.load(f)

def ensure_folder(path: str):
    os.makedirs(path, exist_ok=True)
