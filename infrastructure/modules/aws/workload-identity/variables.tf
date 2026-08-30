variable "name" {
  description = "IAM role/policy name prefix."
  type        = string
}

variable "cluster_name" {
  description = "EKS cluster name."
  type        = string
}

variable "namespace" {
  description = "Kubernetes namespace bound to the IAM role."
  type        = string
}

variable "service_account" {
  description = "Kubernetes service account bound to the IAM role."
  type        = string
}

variable "policy_json" {
  description = "Least-privilege IAM policy JSON for the workload."
  type        = string
}

variable "tags" {
  description = "Tags applied to IAM resources."
  type        = map(string)
  default     = {}
}
