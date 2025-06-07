// Can collect outputs, such as the IDs of created resources etc. 
output "vpc_id" {
  value = aws_vpc.Ducklake_VPC.id
}
