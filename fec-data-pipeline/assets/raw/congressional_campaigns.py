"""@bruin
name: raw.congressional_campaigns
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
  - name: CAND_ID
    type: string
    description: Candidate ID
    primary_key: true
  - name: CAND_NAME
    type: string
    description: Candidate name
  - name: CAND_ICI
    type: string
    description: Incumbent/challenger/open seat status
  - name: PTY_CD
    type: string
    description: Party code
  - name: CAND_PTY_AFFILIATION
    type: string
    description: Party affiliation
  - name: TTL_RECEIPTS
    type: float
    description: Total receipts (USD)
  - name: TRANS_FROM_AUTH
    type: float
    description: Transfers from authorized committees (USD)
  - name: TTL_DISB
    type: float
    description: Total disbursements (USD)
  - name: TRANS_TO_AUTH
    type: float
    description: Transfers to authorized committees (USD)
  - name: COH_BOP
    type: float
    description: Cash on hand at beginning of period (USD)
  - name: COH_COP
    type: float
    description: Cash on hand at close of period (USD)
  - name: CAND_CONTRIB
    type: float
    description: Contributions from candidate (USD)
  - name: CAND_LOANS
    type: float
    description: Loans from candidate (USD)
  - name: OTHER_LOANS
    type: float
    description: Other loans (USD)
  - name: CAND_LOAN_REPAY
    type: float
    description: Candidate loan repayments (USD)
  - name: OTHER_LOAN_REPAY
    type: float
    description: Other loan repayments (USD)
  - name: DEBTS_OWED_BY
    type: float
    description: Debts owed by candidate (USD)
  - name: TTL_INDIV_CONTRIB
    type: float
    description: Total individual contributions (USD)
  - name: CAND_OFFICE_ST
    type: string
    description: State of office sought
  - name: CAND_OFFICE_DISTRICT
    type: string
    description: Congressional district
  - name: SPEC_ELECTION
    type: string
    description: Special election status
  - name: PRIM_ELECTION
    type: string
    description: Primary election status
  - name: RUN_ELECTION
    type: string
    description: Runoff election status
  - name: GEN_ELECTION
    type: string
    description: General election status
  - name: GEN_ELECTION_PRECENT
    type: float
    description: General election vote percentage
  - name: OTHER_POL_CMTE_CONTRIB
    type: float
    description: Contributions from other political committees (USD)
  - name: POL_PTY_CONTRIB
    type: float
    description: Contributions from party committees (USD)
  - name: CVG_END_DT
    type: timestamp
    description: Coverage end date
  - name: INDIV_REFUNDS
    type: float
    description: Refunds to individuals (USD)
  - name: CMTE_REFUNDS
    type: float
    description: Refunds to committees (USD)
@bruin"""

# Summary financial information for each House/Senate campaign committee.
# One record per.

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
    blob_name = f"fec/congressional_campaigns/{date_partition}/webl{yy}.zip"

    client = _gcs_client()
    zip_bytes = io.BytesIO(client.bucket(GCS_BUCKET).blob(blob_name).download_as_bytes())

    column_names = [
        "CAND_ID", "CAND_NAME", "CAND_ICI", "PTY_CD", "CAND_PTY_AFFILIATION",
        "TTL_RECEIPTS", "TRANS_FROM_AUTH", "TTL_DISB", "TRANS_TO_AUTH",
        "COH_BOP", "COH_COP", "CAND_CONTRIB", "CAND_LOANS", "OTHER_LOANS",
        "CAND_LOAN_REPAY", "OTHER_LOAN_REPAY", "DEBTS_OWED_BY", "TTL_INDIV_CONTRIB",
        "CAND_OFFICE_ST", "CAND_OFFICE_DISTRICT", "SPEC_ELECTION", "PRIM_ELECTION",
        "RUN_ELECTION", "GEN_ELECTION", "GEN_ELECTION_PRECENT", "OTHER_POL_CMTE_CONTRIB",
        "POL_PTY_CONTRIB", "CVG_END_DT", "INDIV_REFUNDS", "CMTE_REFUNDS",
    ]

    with zipfile.ZipFile(zip_bytes) as z:
        txt_file = next(f for f in z.namelist() if f.endswith(".txt"))
        with z.open(txt_file) as f:
            df = pd.read_csv(f, sep="|", header=None, names=column_names, dtype=str)

    str_cols = [
        "CAND_ID", "CAND_NAME", "CAND_ICI", "PTY_CD", "CAND_PTY_AFFILIATION",
        "CAND_OFFICE_ST", "CAND_OFFICE_DISTRICT", "SPEC_ELECTION", "PRIM_ELECTION",
        "RUN_ELECTION", "GEN_ELECTION",
    ]
    for c in str_cols:
        df[c] = df[c].fillna("").str.strip()

    float_cols = [
        "TTL_RECEIPTS", "TRANS_FROM_AUTH", "TTL_DISB", "TRANS_TO_AUTH",
        "COH_BOP", "COH_COP", "CAND_CONTRIB", "CAND_LOANS", "OTHER_LOANS",
        "CAND_LOAN_REPAY", "OTHER_LOAN_REPAY", "DEBTS_OWED_BY", "TTL_INDIV_CONTRIB",
        "OTHER_POL_CMTE_CONTRIB", "POL_PTY_CONTRIB", "INDIV_REFUNDS", "CMTE_REFUNDS",
        "GEN_ELECTION_PRECENT",
    ]
    for c in float_cols:
        df[c] = pd.to_numeric(df[c].str.strip().str.replace(",", "", regex=False), errors="coerce")

    df["CVG_END_DT"] = pd.to_datetime(df["CVG_END_DT"].str.strip(), format="%m/%d/%Y", errors="coerce")

    return df[column_names]
