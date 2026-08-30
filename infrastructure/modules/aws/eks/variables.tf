variable "name" {
  description = "EKS cluster name."
  type        = string
}

variable "kubernetes_version" {
  description = "EKS Kubernetes minor version."
  type        = string
}

variable "subnet_ids" {
  description = "Private subnet IDs used by EKS control plane and nodes."
  type        = list(string)
}

variable "platform_admin_principal_arn" {
  description = "IAM principal granted cluster-admin access through the EKS access API."
  type        = string
}

variable "kms_key_arn" {
  description = "Optional KMS key used to encrypt Kubernetes secrets."
  type        = string
  default     = null
  nullable    = true
}

variable "endpoint_public_access" {
  description = "Expose the Kubernetes API endpoint publicly."
  type        = bool
  default     = false
}

variable "node_instance_types" {
  description = "Managed node-group instance types."
  type        = list(string)
  default     = ["m7i.large"]
}

variable "node_min_size" {
  type    = number
  default = 2
}

variable "node_desired_size" {
  type    = number
  default = 2
}

variable "node_max_size" {
  type    = number
  default = 6
}

variable "log_retention_days" {
  description = "CloudWatch retention for EKS control-plane logs."
  type        = number
  default     = 30
}

variable "tags" {
  description = "Tags applied to EKS resources."
  type        = map(string)
  default     = {}
}
