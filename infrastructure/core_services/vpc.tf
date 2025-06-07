# create a VPC (Virtual Private Cloud) which is your own virtual Private Network
resource "aws_vpc" "Ducklake_VPC" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true
  tags = {
    Name = "Ducklake_VPC"
  }
}

# A Virtual Private Cloud (VPC) is a logically isolated section of the AWS cloud where you can 
# launch AWS resources in a virtual network that you define
# The cidr_block defines the IP address range for the VPC. In this case, 10.0.0.0/16 allows for a range of IP 
# addresses from 10.0.0.0 to 10.0.255.255, providing a large range of possible IP addresses within the VPC


# A subnet is a range of IP addresses within your VPC. A subnet can be designated as public or private 
# based on whether it has access to the internet (via an internet gateway)
resource "aws_subnet" "ducklake_public" {
  vpc_id            = aws_vpc.Ducklake_VPC.id
  cidr_block        = "10.0.1.0/24" // smaller CIDR range within the VPC 
  availability_zone = "eu-west-2a"  // Change to your desired AZ. which AWS data center the subnet resides
}

# Internet Gateway (IGW) allows communication between instances in VPC and the internet
# It serves as a connection point between the VPC and the outside world
resource "aws_internet_gateway" "ducklake_igw" {
  vpc_id = aws_vpc.Ducklake_VPC.id
}

# A route table contains a set of rules, called routes, that determine where network traffic is directed.
# The route block specifies that all traffic (0.0.0.0/0, which is a shorthand for any IP address) should be 
# routed to the Internet Gateway (gateway_id = aws_internet_gateway.igw.id), effectively allowing internet 
# access for resources in the associated subnet
resource "aws_route_table" "ducklake_route_public" {
  vpc_id = aws_vpc.Ducklake_VPC.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.ducklake_igw.id
  }
}

# This resource links a specific subnet with a route table
# The subnet_id specifies the subnet (created earlier) that this route table will be associated with
# The route_table_id specifies which route table (created earlier) will be associated with the subnet
resource "aws_route_table_association" "ducklake_route_table_public" {
  subnet_id      = aws_subnet.ducklake_public.id
  route_table_id = aws_route_table.ducklake_route_public.id
}
