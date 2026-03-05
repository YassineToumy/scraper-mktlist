"""
Backblaze B2 image storage.
Downloads images from source URLs and uploads to B2.
Falls back to original URL if B2 is not configured or upload fails.
"""

import os
import hashlib
import logging
import requests
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

B2_KEY_ID = os.getenv("B2_KEY_ID", "")
B2_APPLICATION_KEY = os.getenv("B2_APPLICATION_KEY", "")
B2_ENDPOINT = os.getenv("B2_ENDPOINT", "https://s3.us-east-005.backblazeb2.com")
B2_BUCKET = os.getenv("B2_BUCKET", "immo-world")

log = logging.getLogger("storage")

_s3_client = None


def _get_s3():
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client(
            "s3",
            endpoint_url=B2_ENDPOINT,
            aws_access_key_id=B2_KEY_ID,
            aws_secret_access_key=B2_APPLICATION_KEY,
        )
    return _s3_client


def _b2_configured():
    return bool(B2_KEY_ID and B2_APPLICATION_KEY)


def _make_key(source: str, ad_id: str, img_url: str, index: int) -> str:
    ext = img_url.split("?")[0].rsplit(".", 1)[-1].lower()
    if ext not in ("jpg", "jpeg", "png", "webp", "gif", "avif"):
        ext = "jpg"
    url_hash = hashlib.md5(img_url.encode()).hexdigest()[:8]
    return f"{source}/{ad_id}/{index:03d}_{url_hash}.{ext}"


def _public_url(key: str) -> str:
    return f"{B2_ENDPOINT}/{B2_BUCKET}/{key}"


def upload_image(source: str, ad_id: str, img_url: str, index: int = 0, timeout: int = 20) -> str:
    """
    Upload one image to B2. Returns B2 public URL.
    Falls back to original URL on any failure.
    Skips upload if image already exists in B2.
    """
    if not _b2_configured() or not img_url:
        return img_url

    try:
        s3 = _get_s3()
        key = _make_key(source, str(ad_id), img_url, index)

        # Check if already uploaded — skip re-upload
        try:
            s3.head_object(Bucket=B2_BUCKET, Key=key)
            return _public_url(key)
        except ClientError:
            pass  # Not yet uploaded, proceed

        # Download from source
        resp = requests.get(img_url, timeout=timeout, stream=True, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        })
        resp.raise_for_status()
        content_type = resp.headers.get("Content-Type", "image/jpeg").split(";")[0]
        data = resp.content

        if not data:
            return img_url

        # Upload to B2
        s3.put_object(
            Bucket=B2_BUCKET,
            Key=key,
            Body=data,
            ContentType=content_type,
        )
        return _public_url(key)

    except Exception as e:
        log.warning(f"B2 upload failed for {img_url[:80]}: {e}")
        return img_url  # fallback to original URL


def upload_images(source: str, ad_id: str, img_urls: list, timeout: int = 20) -> list:
    """
    Upload multiple images to B2. Returns list of B2 URLs.
    Falls back to original URL for each failed upload.
    """
    if not img_urls:
        return []
    return [
        upload_image(source, ad_id, url, index=i, timeout=timeout)
        for i, url in enumerate(img_urls)
        if url
    ]
