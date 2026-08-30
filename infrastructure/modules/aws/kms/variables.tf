variable "name" {
  description = "Logical key name used for the alias."
  type        = string
}

variable "description" {
  description = "KMS key description."
  type        = string
  default     = "Open Data Platform data encryption key"
}

variable "deletion_window_in_days" {
  description = "KMS deletion waiting period."
  type        = number
  default     = 30

  validation {
    condition     = var.deletion_window_in_days >= 7 && var.deletion_window_in_days <= 30
    error_message = "KMS deletion window must be between 7 and 30 days."
  }
}

variable "tags" {
  description = "Tags applied to KMS resources."
  type        = map(string)
  default     = {}
}
