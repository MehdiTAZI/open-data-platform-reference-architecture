locals {
  common_tags = merge(var.tags, {
    "odp.io/managed-by" = "opentofu"
    "odp.io/capability" = "workload-identity"
  })
}

data "aws_iam_policy_document" "assume" {
  statement {
    actions = ["sts:AssumeRole", "sts:TagSession"]

    principals {
      type        = "Service"
      identifiers = ["pods.eks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "this" {
  name_prefix        = "${var.name}-"
  assume_role_policy = data.aws_iam_policy_document.assume.json
  tags               = local.common_tags
}

resource "aws_iam_policy" "this" {
  name_prefix = "${var.name}-"
  policy      = var.policy_json
  tags        = local.common_tags
}

resource "aws_iam_role_policy_attachment" "this" {
  role       = aws_iam_role.this.name
  policy_arn = aws_iam_policy.this.arn
}

resource "aws_eks_pod_identity_association" "this" {
  cluster_name    = var.cluster_name
  namespace       = var.namespace
  service_account = var.service_account
  role_arn        = aws_iam_role.this.arn
}
