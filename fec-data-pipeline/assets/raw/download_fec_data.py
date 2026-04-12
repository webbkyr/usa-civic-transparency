"""@bruin
name: raw.download_fec_data
type: python
description: "Download FEC bulk data files that drive the campaign contribution transparency dashboard."
depends: []
secrets:
  - key: gcp-default
    inject_as: GCP_CREDS
@bruin"""

import json
import logging
import os
import tempfile
from datetime import datetime, timezone

import requests
from google.cloud import storage
from google.oauth2 import service_account

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

GCS_BUCKET = os.environ.get("GCS_BUCKET_NAME", "civic-public-datasets")

FEC_BASE_URL = "https://www.fec.gov/files/bulk-downloads/{year}/"


def execution_date() -> datetime:
    raw = os.environ.get("BRUIN_START_DATE")
    if raw:
        return datetime.fromisoformat(raw)
    return datetime.now(timezone.utc)


def fec_files(year: int) -> list[dict]:
    yy = str(year)[-2:]
    return [
        {"filename": f"webl{yy}.zip", "gcs_prefix": "congressional_campaigns"},
        {"filename": f"pas2{yy}.zip", "gcs_prefix": "committee_contributions"},
        {"filename": f"cn{yy}.zip",   "gcs_prefix": "candidates"},
        {"filename": f"cm{yy}.zip",   "gcs_prefix": "committees"},
    ]


def gcs_client() -> storage.Client:
    creds = json.loads(os.environ["GCP_CREDS"])
    credentials = service_account.Credentials.from_service_account_info(
        json.loads(creds["service_account_json"])
    )
    return storage.Client(credentials=credentials, project=creds["project_id"])


def source_last_modified(url: str) -> datetime | None:
    r = requests.head(url, timeout=30)
    r.raise_for_status()
    lm = r.headers.get("Last-Modified")
    if lm:
        return datetime.strptime(lm, "%a, %d %b %Y %H:%M:%S %Z").replace(tzinfo=timezone.utc)
    return None


def download(url: str, dest: str) -> None:
    with requests.get(url, stream=True, timeout=300) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)


def should_upload(blob: storage.Blob, src_modified: datetime | None) -> bool:
    if not blob.exists():
        return True
    if src_modified is None:
        return False
    blob.reload()
    stored = (blob.metadata or {}).get("source_last_modified")
    if not stored:
        return True
    return src_modified > datetime.fromisoformat(stored)


dt = execution_date()
year = dt.year
date_partition = f"year={dt.year:04d}/month={dt.month:02d}/day={dt.day:02d}"

client   = gcs_client()
bucket   = client.bucket(GCS_BUCKET)
base_url = FEC_BASE_URL.format(year=year)

with tempfile.TemporaryDirectory() as tmpdir:
    for fec_file in fec_files(year):
        source_url = base_url + fec_file["filename"]
        blob_name = f"fec/{fec_file['gcs_prefix']}/{date_partition}/{fec_file['filename']}"
        blob = bucket.blob(blob_name)

        logging.info("Checking source: %s", source_url)
        src_modified = source_last_modified(source_url)
        logging.info("Source Last-Modified: %s", src_modified)

        if not should_upload(blob, src_modified):
            logging.info("Up to date, skipping: gs://%s/%s", GCS_BUCKET, blob_name)
            continue

        local_path = os.path.join(tmpdir, fec_file["filename"])
        logging.info("Downloading %s ...", source_url)
        download(source_url, local_path)

        blob.metadata = {"source_last_modified": src_modified.isoformat() if src_modified else ""}
        blob.upload_from_filename(local_path)
        logging.info("Done: gs://%s/%s", GCS_BUCKET, blob_name)
