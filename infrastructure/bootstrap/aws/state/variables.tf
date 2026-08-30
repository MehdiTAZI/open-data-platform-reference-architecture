variable "region" {
  description = "AWS region for the OpenTofu state backend."
  type        = string
}

variable "state_bucket_name" {
  description = "Globally unique S3 bucket name used for OpenTofu state."
  type        = string
}

variable "lock_table_name" {
  description = "DynamoDB table used for state locking."
  type        = string
  default     = "odp-opentofu-locks"
}

variable "tags" {
  description = "Tags applied to bootstrap resources."
  type        = map(string)
  default     = {}
}
