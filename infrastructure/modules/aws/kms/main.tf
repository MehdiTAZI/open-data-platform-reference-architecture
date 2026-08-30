locals {
  common_tags = merge(var.tags, {
    "odp.io/managed-by" = "opentofu"
    "odp.io/capability" = "encryption"
  })
}

resource "aws_kms_key" "this" {
  description             = var.description
  deletion_window_in_days = var.deletion_window_in_days
  enable_key_rotation     = true
  multi_region            = false

  tags = local.common_tags
}

resource "aws_kms_alias" "this" {
  name          = "alias/${var.name}"
  target_key_id = aws_kms_key.this.key_id
}
