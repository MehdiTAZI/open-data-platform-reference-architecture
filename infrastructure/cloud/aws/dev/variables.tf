variable "region" {
  description = "AWS region."
  type        = string
  default     = "eu-west-1"
}

variable "name" {
  description = "Platform environment name."
  type        = string
  default     = "odp-dev"
}

variable "platform_admin_principal_arn" {
  description = "IAM principal granted EKS cluster-admin access."
  type        = string
}

variable "enable_nat_gateways" {
  description = "Create one NAT gateway per AZ for private workload egress."
  type        = bool
  default     = true
}

variable "tags" {
  description = "Additional environment tags."
  type        = map(string)
  default     = {}
}
