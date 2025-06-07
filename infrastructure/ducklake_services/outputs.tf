output "s3_bucket_names" {
  value = [for b in aws_s3_bucket.buckets : b.bucket]
}

output "ec2_public_ip" {
  value = aws_instance.ducklake_ec2.public_ip
}

output "rds_endpoint" {
  value = aws_db_instance.ducklake_postgres.endpoint
}

