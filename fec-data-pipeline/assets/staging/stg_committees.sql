/* @bruin
name: staging.stg_committees
type: bq.sql
depends:
- raw.committees
materialization:
  type: table
columns:
  - name: id
    type: VARCHAR
    description: Committee Id
    primary_key: true
    nullable: false
  - name: name
    type: VARCHAR
    description: Committee name
    nullable: false
  - name: cmte_dsgn
    type: VARCHAR
    description: Committee designation. Selecting only for superpacs.
    nullable: false
  - name: committee_type
    type: VARCHAR
    description: Committee type.
    nullable: false
  - name: party_affiliation
    type: VARCHAR
    description: Commitee party affiliation, if applicable.
  - name: associated_org_name
    type: VARCHAR
    description: Name of connected organization, if applicable. This is who is behind the PAC.
  - name: cand_id
    type: VARCHAR
    description: Candidate Id that the committee is associated with, if applicable.
@bruin */


-- Commitee designation
-- # A = Authorized by a candidate
-- # B = Lobbyist/Registrant PAC
-- # D = Leadership PAC
-- # J = Joint fundraiser
-- # P = Principal campaign committee of a candidate
-- # U = Unauthorized

-- ORG_TP
-- C = Corporation
-- L = Labor organization
-- M = Membership organization
-- T = Trade association
-- V = Cooperative
-- W = Corporation without capital stock
-- 

-- Identifying the SuperPACs
-- Relevant committee types (cmte_tp)
-- O (Super PAC) — the classic Super PAC, can raise unlimited funds from corporations, unions, and individuals, but can only make independent expenditures. Cannot coordinate with candidates.
-- U (Single-candidate independent expenditure) — similar to a Super PAC but focused on a single candidate. Still independent, still unlimited fundraising.
SELECT 
  cmte_id as id,
  cmte_nm as name,
  cmte_dsgn,
  cmte_tp as committee_type,
  NULLIF(cmte_pty_affiliation, '') as party_affiliation,
--   NULLIF(org_tp, '') as org_type,
--   org_type is always null for Super PACs.
--   The org_tp field is probably more relevant for traditional PACs that are directly connected to a corporation, union, or trade association.
--   case org_tp
--     when 'C' then 'Corporation'
--     when 'L' then 'Labor organization'
--     when 'M' then 'Membership organization'
--     when 'T' then 'Trade association'
--     when 'V' then 'Cooperative'
--     when 'W' then 'Corporation without capital stock'
--     else NULL
--   end as org_type_desc,
  NULLIF(connected_org_nm, '') as associated_org_name,
  NULLIF(cand_id, '') as cand_id
FROM raw.committees
WHERE cmte_dsgn IN ('U') AND cmte_tp IN ('O', 'U')