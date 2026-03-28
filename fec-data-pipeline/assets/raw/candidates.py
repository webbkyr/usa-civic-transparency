"""@bruin
name: raw.candidates
connection: gcp-default
depends:
  - raw.download_fec_data
materialization:
  type: table
  table_name: raw.candidates
  strategy: create+replace
image: python:3.11
secrets:
  - key: gcp-default
    inject_as: GCP_CREDS
columns:
  - name: CAND_ID
    type: string
    description: Candidate ID
    primary_key: true
  - name: CAND_NAME
    type: string
    description: Candidate name
  - name: CAND_PTY_AFFILIATION
    type: string
    description: Party affiliation
  - name: CAND_ELECTION_YR
    type: integer
    description: Year of election
  - name: CAND_OFFICE_ST
    type: string
    description: State of office sought
  - name: CAND_OFFICE
    type: string
    description: "Office sought (H=House, S=Senate, P=President)"
  - name: CAND_OFFICE_DISTRICT
    type: string
    description: Congressional district
  - name: CAND_ICI
    type: string
    description: "Incumbent/challenger/open seat (I=incumbent, C=challenger, O=open seat)"
  - name: CAND_STATUS
    type: string
    description: "Candidate status (C=statutory candidate, F=candidate for future election, N=not yet a candidate, P=prior candidate)"
  - name: CAND_PCC
    type: string
    description: Principal campaign committee ID
  - name: CAND_ST1
    type: string
    description: Mailing address street 1
  - name: CAND_ST2
    type: string
    description: Mailing address street 2
  - name: CAND_CITY
    type: string
    description: Mailing address city
  - name: CAND_ST
    type: string
    description: Mailing address state
  - name: CAND_ZIP
    type: string
    description: Mailing address zip code
@bruin"""

import io
import json
import os
import zipfile
from datetime import datetime, timezone

import pandas as pd
from google.cloud import storage
from google.oauth2 import service_account

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
    blob_name = f"fec/candidates/{date_partition}/cn{yy}.zip"

    client = _gcs_client()
    zip_bytes = io.BytesIO(client.bucket(GCS_BUCKET).blob(blob_name).download_as_bytes())

    column_names = [
        "CAND_ID", "CAND_NAME", "CAND_PTY_AFFILIATION", "CAND_ELECTION_YR",
        "CAND_OFFICE_ST", "CAND_OFFICE", "CAND_OFFICE_DISTRICT", "CAND_ICI",
        "CAND_STATUS", "CAND_PCC", "CAND_ST1", "CAND_ST2", "CAND_CITY",
        "CAND_ST", "CAND_ZIP",
    ]

    with zipfile.ZipFile(zip_bytes) as z:
        txt_file = next(f for f in z.namelist() if f.endswith(".txt"))
        with z.open(txt_file) as f:
            df = pd.read_csv(f, sep="|", header=None, names=column_names, dtype=str)

    str_cols = [
        "CAND_ID", "CAND_NAME", "CAND_PTY_AFFILIATION", "CAND_OFFICE_ST",
        "CAND_OFFICE", "CAND_OFFICE_DISTRICT", "CAND_ICI", "CAND_STATUS",
        "CAND_PCC", "CAND_ST1", "CAND_ST2", "CAND_CITY", "CAND_ST", "CAND_ZIP",
    ]
    for c in str_cols:
        df[c] = df[c].fillna("").str.strip()

    df["CAND_ELECTION_YR"] = pd.to_numeric(df["CAND_ELECTION_YR"].str.strip(), errors="coerce").astype("Int64")

    return df[column_names]
