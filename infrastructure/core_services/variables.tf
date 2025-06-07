# # each variable below, does not need specifying actual value, 
# as a corresponding TF_VAR_ value exists
# # within .bashrc, so terraform can pick it up through this method automatically 
variable "AWS_ACCESS_KEY_ID" {
  type = string
}
variable "AWS_SECRET_ACCESS_KEY" {
  type = string
}
variable "AWS_DEFAULT_REGION" {
  type    = string
  default = "eu-west-2" # this is the London Region
}