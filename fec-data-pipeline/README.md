# USA Civic Transparency Dashboard

## FEC Data Pipeline
raw.download_fec_data
  │
  ├── gcs/webl{yy}.zip  => raw.congressional_campaigns (BQ)
  ├── gcs/pas2{yy}.zip  => raw.committee_contributions (BQ)
  └── gcs/cn{yy}.zip"   => raw.candidates (BQ)
TODO:
- problem statement
- architecture diagram
- context of the dataset
- reproducible with tf variable examples
- tests
- Looker Studio 
- github actions deployment & CI/CD
- future enhancements

ideas:
- make bootstrap
  - terraform apply (create resources)
- make teardown
  - terraform destroy (delete )

- 