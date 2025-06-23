#!/bin/bash  

# destroy all other IaC on AWS by Terraform
terraform destroy -var-file="db_secrets.tfvars" -var-file="ip.tfvars" -auto-approve 

echo ""
echo "==================================================="
echo "All AWS resources have been successfully destroyed"
echo "==================================================="