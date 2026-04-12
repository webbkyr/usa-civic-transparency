/* @bruin
name: staging.stg_candidates
type: bq.sql
depends:
- raw.candidates

materialization:
  type: table

columns:
  - name: id
    type: VARCHAR
    description: Candidate Id
    primary_key: true
    nullable: false
  - name: last_name
    type: VARCHAR
    description: Candidate last name
    nullable: false
  - name: given_name
    type: VARCHAR
    description: Candidate given name
    nullable: false
  - name: party
    type: VARCHAR
    description: Candidate party affiliation
    nullable: false
  - name: election_year
    type: INTEGER
    description: Election year
    nullable: false
  - name: state
    type: VARCHAR
    description: Candidate state
  - name: office
    type: VARCHAR
    description: Office for which the candidate is running
  - name: district
    type: VARCHAR
    description: Candidate district
  - name: incumbent_status
    type: VARCHAR
    description: Office holder incumbency status
  - name: cand_status
    type: VARCHAR
    description: Status of FEC registration.
  - name: principal_campaign_committee_id
    type: VARCHAR
    description: Campaign committee id

@bruin */
 -- cleaned, filtered, renamed


SELECT 
  cand_id as id,
  SPLIT(cand_name, ',')[SAFE_OFFSET(0)] AS last_name,
  SPLIT(cand_name, ',')[SAFE_OFFSET(1)] AS given_name,
  cand_pty_affiliation as party,
  cand_election_yr as election_year,
  cand_office_st as state,
  CASE cand_office
    WHEN 'H' THEN 'house'
    WHEN 'P' THEN 'president'
    WHEN 'S' THEN 'senate'
    ELSE 'unknown'
  END AS office,
  cand_office_district as district,
  CASE cand_ici
    WHEN 'C' THEN 'challenger'
    WHEN 'I' THEN 'incumbent'
    WHEN 'O' THEN 'open'
    ELSE 'unknown'
  END AS incumbent_status,
 cand_status,
 cand_pcc as principal_campaign_committee_id
FROM raw.candidates
WHERE cand_status IN ('C', 'N') -- actively running this cycle or filed paperwork
