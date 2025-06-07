## Infrastructure

This project will use AWS services for the infra.

The infra required will be:

    - S3 buckets (this will be for the data lake storage)
    - An EC2 machine which will run the pipelines
    - An RDS postgres instance which will have database(s) for the catalog metadata in DuckLake


Hidden variables are stored locally (resticted within .gitignore), using the following files:

- db_secrets.tfvars (covers the details for the admin account of the RDS postgres)
- ip.tfvars (covers the IP range that can connect to resources such as the EC2 machine that will run pipelines)

***other info may come later (maybe diagrams?)***