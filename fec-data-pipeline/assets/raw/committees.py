"""@bruin
name: raw.committees
connection: gcp-default
depends:
  - raw.download_fec_data
materialization:
  type: table
  strategy: create+replace
image: python:3.11
secrets:
  - key: gcp-default
    inject_as: GCP_CREDS
columns:
  - name: CMTE_ID
    type: string
    description: Committee ID
    primary_key: true
  - name: CMTE_NM
    type: string
    description: Committee name
  - name: TRES_NM
    type: string
    description: Treasurer name
  - name: CMTE_ST1
    type: string
    description: Street one
  - name: CMTE_ST2
    type: string
    description: Street two
  - name: CMTE_CITY
    type: string
    description: City or town
  - name: CMTE_ST
    type: string
    description: State
  - name: CMTE_ZIP
    type: string
    description: Zip code
  - name: CMTE_DSGN
    type: string
    description: Committee designation
  - name: CMTE_TP
    type: string
    description: Committee type
  - name: CMTE_PTY_AFFILIATION
    type: string
    description: Committee party
  - name: CMTE_FILING_FREQ
    type: string
    description: Filing frequency
  - name: ORG_TP
    type: string
    description: Interest group category
  - name: CONNECTED_ORG_NM
    type: string
    description: Connected organization name
  - name: CAND_ID
    type: string
    description: Candidate ID

@bruin"""

import io
import json
import os
import zipfile
from datetime import datetime, timezone

import pandas as pd
from google.cloud import storage
from google.oauth2 import service_account
# When a committee has a committee type designation of H, S, or P, the candidate's identification number will be entered in this field.

# Filing frequency
# A = Administratively terminated
# D = Debt
# M = Monthly filer
# Q = Quarterly filer
# T = Terminated
# W = Waived

# cm26.zip


GCS_BUCKET = os.environ.get("GCS_BUCKET_NAME", "civic-public-datasets")

def _gcs_client() -> storage.Client:
    creds = json.loads(os.environ["GCP_CREDS"])
    credentials = service_account.Credentials.from_service_account_info(
        json.loads(creds["service_account_json"])
    )
    return storage.Client(credentials=credentials, project=creds["project_id"])


def _execution_dt() -> datetime:
    raw = os.environ.get("BRUIN_START_DATE")
    return datetime.fromisoformat(raw) if raw else datetime.now(timezone.utc)


def materialize() -> pd.DataFrame:
    dt = _execution_dt()
    yy = str(dt.year)[-2:]
    date_partition = f"year={dt.year:04d}/month={dt.month:02d}/day={dt.day:02d}"
    blob_name = f"fec/committees/{date_partition}/cm{yy}.zip"

    client = _gcs_client()
    zip_bytes = io.BytesIO(client.bucket(GCS_BUCKET).blob(blob_name).download_as_bytes())

    column_names = [
        "CMTE_ID", "CMTE_NM", "TRES_NM", "CMTE_ST1", "CMTE_ST2",
        "CMTE_CITY", "CMTE_ST", "CMTE_ZIP", "CMTE_DSGN", "CMTE_TP",
        "CMTE_PTY_AFFILIATION", "CMTE_FILING_FREQ", "ORG_TP",
        "CONNECTED_ORG_NM", "CAND_ID",
    ]

    with zipfile.ZipFile(zip_bytes) as z:
        txt_file = next(f for f in z.namelist() if f.endswith(".txt"))
        with z.open(txt_file) as f:
            df = pd.read_csv(f, sep="|", header=None, names=column_names, dtype=str)

    str_cols = [
        "CMTE_ID", "CMTE_NM", "TRES_NM", "CMTE_ST1", "CMTE_ST2",
        "CMTE_CITY", "CMTE_ST", "CMTE_ZIP", "CMTE_DSGN", "CMTE_TP",
        "CMTE_PTY_AFFILIATION", "CMTE_FILING_FREQ", "ORG_TP",
        "CONNECTED_ORG_NM", "CAND_ID",
    ]
    for c in str_cols:
        df[c] = df[c].fillna("").str.strip()

    return df[column_names]