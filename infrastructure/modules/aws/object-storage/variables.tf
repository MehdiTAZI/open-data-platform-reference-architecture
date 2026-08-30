variable "bucket_prefix" {
  description = "Prefix used for the globally unique lakehouse bucket."
  type        = string
}

variable "kms_key_arn" {
  description = "KMS key ARN used for default object encryption."
  type        = string
}

variable "noncurrent_version_expiration_days" {
  description = "Retention for non-current object versions."
  type        = number
  default     = 90

  validation {
    condition     = var.noncurrent_version_expiration_days >= 30
    error_message = "Retain non-current object versions for at least 30 days."
  }
}

variable "tags" {
  description = "Tags applied to storage resources."
  type        = map(string)
  default     = {}
}
