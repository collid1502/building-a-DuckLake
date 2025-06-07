resource "aws_s3_bucket" "buckets" {
  for_each = toset(var.bucket_names)

  bucket = each.key

  tags = {
    Name    = each.key
    Project = "ducklake"
  }
}
