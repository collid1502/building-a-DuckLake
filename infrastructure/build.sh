#!/bin/bash  

# ensure formatting in all .tf files
terraform fmt -recursive

# creates AWS Resources through Terraform
terraform validate
terraform plan -var-file="db_secrets.tfvars" -var-file="ip.tfvars"
terraform apply -var-file="db_secrets.tfvars" -var-file="ip.tfvars" #-auto-approve 

echo ""
echo "=============================================="
echo "All resources have been successfully created"
echo "=============================================="