/* @bruin
name: staging.stg_superpac_expenditures
type: bq.sql
depends:
- raw.committee_contributions
- staging.stg_committees
materialization:
  type: table
columns:
  - name: id
    type: VARCHAR
    description: Expenditure Id
    primary_key: true
    nullable: false
  - name: committee_id
    type: VARCHAR
    description: Committee Id that made the expenditure. Foreign key to staging.stg_committees.
    nullable: false
  - name: transaction_id
    type: VARCHAR
    description: Transaction Id for the expenditure.
    nullable: false
  - name: candidate_id
    type: VARCHAR
    description: Candidate Id that the expenditure is advocating for or against, if applicable. Foreign key to staging.stg_candidates.
  - name: committee_name
    type: VARCHAR
    description: Name of the committee that made the expenditure.
    nullable: false
  - name: amendment_indicator
    type: VARCHAR
    description: Indicates whether the expenditure record is a new record, an amendment, or a termination. Values are 'New', 'Amendment', or 'Termination'.
    nullable: false
  - name: transaction_type
    type: VARCHAR
    description: Type of the transaction, as reported to the FEC. Foreign key to raw.transaction_type_lookup.
  - name: expenditure_amount
    type: FLOAT
    description: Amount of the expenditure.
  - name: expenditure_date
    type: DATE
    description: Date of the expenditure.
@bruin */

WITH transactions as (
    SELECT 
        c.sub_id as id,
        c.cmte_id as commitee_id,
        c.tran_id as transaction_id,
        c.cand_id as candidate_id,
        sc.name as committee_name,
        case c.amndt_ind
            when 'N' then 'New'
            when 'A' then 'Amendment'
            when 'T' then 'Termination'
            else NULL
        end as amendment_indicator,
        c.transaction_tp as transaction_type,
        c.transaction_amt as expenditure_amount,
        c.transaction_dt as expenditure_date,
       ROW_NUMBER() OVER(
        PARTITION BY c.cmte_id, c.tran_id
        ORDER BY c.sub_id DESC
        ) AS row_num
FROM raw.committee_contributions c 
JOIN staging.stg_committees sc on c.cmte_id = sc.id
)
SELECT * EXCEPT (row_num)
FROM transactions
WHERE 1=1
AND row_num = 1
AND transaction_id IS NOT NULL
AND amendment_indicator IS NOT NULL 
-- FIltering out potential data quality issue. If amendment indicator is null, we don't know how to interpret the transaction (new vs. amendment)