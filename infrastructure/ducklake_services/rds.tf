resource "aws_db_instance" "ducklake_postgres" {
  identifier              = "ducklake-postgres"
  engine                  = "postgres"
  engine_version          = "15"
  instance_class          = "db.t3.micro"
  allocated_storage       = 20
  db_name                 = "ducklake_master"
  username                = var.db_username
  password                = var.db_password
  skip_final_snapshot     = true
  publicly_accessible     = false
  vpc_security_group_ids  = [aws_security_group.sg_rds.id]
  db_subnet_group_name    = aws_db_subnet_group.ducklake_subnet_group.name
  backup_retention_period = 7             # Retain backups for 7 days
  backup_window           = "03:00-04:00" # Daily backup window (UTC)


  tags = {
    Name = "ducklake-db"
  }
}

resource "aws_db_subnet_group" "ducklake_subnet_group" {
  name       = "ducklake-subnet-group"
  subnet_ids = var.subnet_ids

  tags = {
    Name = "ducklake-db-subnet-group"
  }
}
