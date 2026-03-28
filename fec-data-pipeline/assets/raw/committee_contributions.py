"""@bruin
name: raw.committee_contributions
connection: gcp-default
depends:
  - raw.download_fec_data
materialization:
  type: table
  table_name: raw.committee_contributions
  strategy: create+replace
image: python:3.11
secrets:
  - key: gcp-default
    inject_as: GCP_CREDS
columns:
  - name: CMTE_ID
    type: string
    description: Filer committee ID
    primary_key: false
  - name: AMNDT_IND
    type: string
    description: Amendment indicator (N=new, A=amendment, T=termination)
  - name: RPT_TP
    type: string
    description: Report type
  - name: TRANSACTION_PGI
    type: string
    description: Primary-general indicator
  - name: IMAGE_NUM
    type: string
    description: Image number
  - name: TRANSACTION_TP
    type: string
    description: "Transaction type (24A=independent expenditure against, 24E=independent expenditure for)"
  - name: ENTITY_TP
    type: string
    description: "Entity type (CCM=candidate committee, COM=committee, PAC=super PAC, etc.)"
  - name: NAME
    type: string
    description: Contributor or committee name
  - name: CITY
    type: string
    description: City
  - name: STATE
    type: string
    description: State
  - name: ZIP_CODE
    type: string
    description: Zip code
  - name: EMPLOYER
    type: string
    description: Employer
  - name: OCCUPATION
    type: string
    description: Occupation
  - name: TRANSACTION_DT
    type: timestamp
    description: Transaction date
  - name: TRANSACTION_AMT
    type: float
    description: Transaction amount (USD)
  - name: OTHER_ID
    type: string
    description: ID of the recipient committee or candidate
  - name: CAND_ID
    type: string
    description: Candidate ID (null for non-candidate expenditures)
  - name: TRAN_ID
    type: string
    description: Transaction ID
    primary_key: true
  - name: FILE_NUM
    type: integer
    description: File number
  - name: MEMO_CD
    type: string
    description: Memo code
  - name: MEMO_TEXT
    type: string
    description: Memo text
  - name: SUB_ID
    type: integer
    description: FEC record number (unique)
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
    blob_name = f"fec/committee_contributions/{date_partition}/pas2{yy}.zip"

    client = _gcs_client()
    zip_bytes = io.BytesIO(client.bucket(GCS_BUCKET).blob(blob_name).download_as_bytes())

    column_names = [
        "CMTE_ID", "AMNDT_IND", "RPT_TP", "TRANSACTION_PGI", "IMAGE_NUM",
        "TRANSACTION_TP", "ENTITY_TP", "NAME", "CITY", "STATE", "ZIP_CODE",
        "EMPLOYER", "OCCUPATION", "TRANSACTION_DT", "TRANSACTION_AMT",
        "OTHER_ID", "CAND_ID", "TRAN_ID", "FILE_NUM", "MEMO_CD", "MEMO_TEXT", "SUB_ID"
    ]

    with zipfile.ZipFile(zip_bytes) as z:
        txt_file = next(f for f in z.namelist() if f.endswith(".txt"))
        with z.open(txt_file) as f:
            df = pd.read_csv(f, sep="|", header=None, names=column_names, dtype=str)

    str_cols = [
        "CMTE_ID", "AMNDT_IND", "RPT_TP", "TRANSACTION_PGI", "IMAGE_NUM",
        "TRANSACTION_TP", "ENTITY_TP", "NAME", "CITY", "STATE", "ZIP_CODE",
        "EMPLOYER", "OCCUPATION", "OTHER_ID", "CAND_ID", "TRAN_ID", "MEMO_CD", "MEMO_TEXT",
    ]
    for c in str_cols:
        df[c] = df[c].fillna("").str.strip()

    df["TRANSACTION_AMT"] = pd.to_numeric(
        df["TRANSACTION_AMT"].str.strip().str.replace(",", "", regex=False), errors="coerce"
    )
    df["TRANSACTION_DT"] = pd.to_datetime(df["TRANSACTION_DT"].str.strip(), format="%m%d%Y", errors="coerce")

    for c in ["FILE_NUM", "SUB_ID"]:
        df[c] = pd.to_numeric(df[c].str.strip(), errors="coerce").astype("Int64")

    return df[column_names]
