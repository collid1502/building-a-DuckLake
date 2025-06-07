#!/bin/bash  

# destroy all other IaC on AWS by Terraform
terraform destroy -auto-approve 

echo ""
echo "==================================================="
echo "All AWS resources have been successfully destroyed"
echo "==================================================="