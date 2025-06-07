variable "aws_region" {
  type        = string
  description = "AWS region to deploy resources"
}

variable "vpc_id" {
  type        = string
  description = "ID of the existing VPC"
}

variable "subnet_ids" {
  type        = list(string)
  description = "List of subnet IDs to use"
}

variable "bucket_names" {
  type        = list(string)
  description = "List of S3 bucket names"
}

variable "instance_type" {
  type        = string
  default     = "t3.micro"
  description = "EC2 instance type"
}

variable "db_username" {
  type = string
}

variable "db_password" {
  type      = string
  sensitive = true
}

variable "bastion_ingress_ip" {
  type        = string
  description = "CIDR block allowed to SSH into EC2"
}