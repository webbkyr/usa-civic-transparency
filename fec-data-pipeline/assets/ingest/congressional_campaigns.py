"""@bruin
name: ingest.congressional_campaigns
connection: warehouse
materialization:
  type: table
  table_name: raw.congressional_campaigns
  strategy: create+replace
image: python:3.11
columns:
  - name: CAND_ID
    type: string
    description: The candidate ID of the congressional campaign
    primary_key: true
  - name: CAND_NAME
    type: string
    description: The name of the congressional campaign
  - name: CAND_ICI
    type: string
    description: Incumbent challenger status
  - name: PTY_CD
    type: string
    description: Party code
  - name: CAND_PTY_AFFILIATION
    type: string
    description: Party affiliation
  - name: TTL_RECEIPTS
    type: integer
    description: Total receipts
  - name: TRANS_FROM_AUTH
    type: integer
    description: Transfers from authorized committees
  - name: TTL_DISB
    type: integer
    description: Total disbursements
  - name: TRANS_TO_AUTH
    type: integer
    description: Transfers to authorized committees
  - name: COH_BOP
    type: integer
    description: Beginning cash
  - name: COH_COP
    type: integer
    description: Ending cash
  - name: CAND_CONTRIB
    type: integer
    description: Contributions from candidate
  - name: CAND_LOANS
    type: integer
    description: Loans from candidate
  - name: OTHER_LOANS
    type: integer
    description: Other loans
  - name: CAND_LOAN_REPAY
    type: integer
    description: Loan repayments from candidate
  - name: OTHER_LOAN_REPAY
    type: integer
    description: Other loan repayments
  - name: DEBTS_OWED_BY
    type: integer
    description: Debts owed by candidate
  - name: TTL_INDIV_CONTRIB
    type: integer
    description: Total individual contributions
  - name: CAND_OFFICE_ST
    type: string
    description: Candidate state
  - name: CAND_OFFICE_DISTRICT
    type: string
    description: Candidate district
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
    type: integer
    description: General election percentage
  - name: OTHER_POL_CMTE_CONTRIB
    type: integer
    description: General primary status
  - name: POL_PTY_CONTRIB
    type: integer
    description: Contributions from party committees
  - name: CVG_END_DT
    type: timestamp
    description: Coverage end date
  - name: INDIV_REFUNDS
    type: integer
    description: Refunds to individuals
  - name: CMTE_REFUNDS
    type: integer
    description: Refunds to committees
@bruin"""

import pandas as pd
import requests
import io
import os
import json

from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import tempfile
import zipfile

WEBL26_ZIP_URL = "https://www.fec.gov/files/bulk-downloads/2026/webl26.zip"

def _download_to_path(url: str, path: str) -> None:
    with requests.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        with open(path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)


def _to_int_nullable(s: str | None, *, scale_if_decimal: int) -> int | None:
    """
    Convert FEC fixed-width Number(p,s) string -> integer.

    If the field contains an explicit decimal point, multiply by `scale_if_decimal`.
    If it does not contain a decimal point, treat it as already scaled (implied decimals).
    """
    if s is None:
        return None
    s = str(s).strip()
    if not s:
        return None

    s = s.replace(",", "")

    sign = -1 if s.startswith("-") else 1
    if s[0] in "+-":
        s = s[1:]

    if not s:
        return None

    try:
        if "." in s:
            dec = Decimal(s)
            scaled = dec * scale_if_decimal
            return sign * int(scaled.to_integral_value(rounding=ROUND_HALF_UP))
        # No decimal point => already scaled (implied decimal digits).
        return sign * int(s)
    except (InvalidOperation, ValueError):
        return None


def materialize() -> pd.DataFrame:
    """
    Load the FEC "All candidates file" (webl26.txt) into `congressional_campaigns`.
    """

    # zip_file_path = 'webl26-2026-03-18.zip'
    text_file_name = 'webl26.txt' # Name of the structured text file inside the zip
    column_names = [
                "CAND_ID",
                "CAND_NAME",
                "CAND_ICI",
                "PTY_CD",
                "CAND_PTY_AFFILIATION",
                "TTL_RECEIPTS",
                "TRANS_FROM_AUTH",
                "TTL_DISB",
                "TRANS_TO_AUTH",
                "COH_BOP",
                "COH_COP",
                "CAND_CONTRIB",
                "CAND_LOANS",
                "OTHER_LOANS",
                "CAND_LOAN_REPAY",
                "OTHER_LOAN_REPAY",
                "DEBTS_OWED_BY",
                "TTL_INDIV_CONTRIB",
                "CAND_OFFICE_ST",
                "CAND_OFFICE_DISTRICT",
                "SPEC_ELECTION",
                "PRIM_ELECTION",
                "RUN_ELECTION",
                "GEN_ELECTION",
                "GEN_ELECTION_PRECENT",
                "OTHER_POL_CMTE_CONTRIB",
                "POL_PTY_CONTRIB",
                "CVG_END_DT",
                "INDIV_REFUNDS",
                "CMTE_REFUNDS",
            ]

    with tempfile.TemporaryDirectory() as tmpdir:
      zip_file_path = os.path.join(tmpdir, "webl26.zip")
      _download_to_path(WEBL26_ZIP_URL, zip_file_path)

      with zipfile.ZipFile(zip_file_path) as z:
          with z.open(text_file_name) as f:
              df = pd.read_csv(f, sep='|', header=None, names=column_names)

          str_columns = [
              "CAND_ID",
              "CAND_NAME",
              "CAND_ICI",
              "PTY_CD",
              "CAND_PTY_AFFILIATION",
              "CAND_OFFICE_ST",
              "CAND_OFFICE_DISTRICT",
              "SPEC_ELECTION",
              "PRIM_ELECTION",
              "RUN_ELECTION",
          ]
          for c in str_columns:
            df[c] = df[c].fillna("").astype("string").str.strip()

          two_dec_columns = [
              "TTL_RECEIPTS",
              "TRANS_FROM_AUTH",
              "TTL_DISB",
              "TRANS_TO_AUTH",
              "COH_BOP",
              "COH_COP",
              "CAND_CONTRIB",
              "CAND_LOANS",
              "OTHER_LOANS",
              "CAND_LOAN_REPAY",
              "OTHER_LOAN_REPAY",
              "DEBTS_OWED_BY",
              "TTL_INDIV_CONTRIB",
              "OTHER_POL_CMTE_CONTRIB",
              "POL_PTY_CONTRIB",
              "INDIV_REFUNDS",
              "CMTE_REFUNDS",
          ]

          for c in two_dec_columns:
              df[c] = df[c].map(lambda v: _to_int_nullable(v, scale_if_decimal=100)).astype("Int64")

          df["GEN_ELECTION_PRECENT"] = (
              df["GEN_ELECTION_PRECENT"]
              .map(lambda v: _to_int_nullable(v, scale_if_decimal=10000))
              .astype("Int64")
          )

          df["CVG_END_DT"] = pd.to_datetime(df["CVG_END_DT"].str.strip(), format="%m/%d/%Y", errors="coerce")

          out_columns = [
              "CAND_ID",
              "CAND_NAME",
              "CAND_ICI",
              "PTY_CD",
              "CAND_PTY_AFFILIATION",
              "TTL_RECEIPTS",
              "TRANS_FROM_AUTH",
              "TTL_DISB",
              "TRANS_TO_AUTH",
              "COH_BOP",
              "COH_COP",
              "CAND_CONTRIB",
              "CAND_LOANS",
              "OTHER_LOANS",
              "CAND_LOAN_REPAY",
              "OTHER_LOAN_REPAY",
              "DEBTS_OWED_BY",
              "TTL_INDIV_CONTRIB",
              "CAND_OFFICE_ST",
              "CAND_OFFICE_DISTRICT",
              "SPEC_ELECTION",
              "PRIM_ELECTION",
              "RUN_ELECTION",
              "GEN_ELECTION",
              "GEN_ELECTION_PRECENT",
              "OTHER_POL_CMTE_CONTRIB",
              "POL_PTY_CONTRIB",
              "CVG_END_DT",
              "INDIV_REFUNDS",
              "CMTE_REFUNDS",
          ]
          return df[out_columns]
