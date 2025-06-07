// Can collect outputs, such as the IDs of created resources etc. 
output "vpc_id" {
  value = aws_vpc.Ducklake_VPC.id
}

output "public_subnet_ids" {
  value = [
    aws_subnet.ducklake_public_1.id,
    aws_subnet.ducklake_public_2.id
  ]
  description = "Public subnet IDs in different AZs"
}
