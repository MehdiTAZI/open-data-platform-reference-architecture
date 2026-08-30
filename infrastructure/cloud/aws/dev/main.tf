provider "aws" {
  region = var.region

  default_tags {
    tags = merge(var.tags, {
      "odp.io/environment" = "dev"
      "odp.io/platform"    = var.name
      "odp.io/managed-by"  = "opentofu"
    })
  }
}

data "aws_availability_zones" "available" {
  state = "available"
}

locals {
  availability_zones = slice(data.aws_availability_zones.available.names, 0, 3)
}

module "network" {
  source = "../../../modules/aws/network"

  name               = var.name
  vpc_cidr           = "10.20.0.0/16"
  availability_zones = local.availability_zones
  public_subnet_cidrs = [
    "10.20.0.0/24",
    "10.20.1.0/24",
    "10.20.2.0/24",
  ]
  private_subnet_cidrs = [
    "10.20.10.0/24",
    "10.20.11.0/24",
    "10.20.12.0/24",
  ]
  enable_nat_gateways = var.enable_nat_gateways
  tags                = var.tags
}

module "data_kms" {
  source = "../../../modules/aws/kms"

  name        = "${var.name}-data"
  description = "${var.name} lakehouse and platform data encryption"
  tags        = var.tags
}

module "lakehouse_storage" {
  source = "../../../modules/aws/object-storage"

  bucket_prefix = "${var.name}-lakehouse-"
  kms_key_arn   = module.data_kms.key_arn
  tags          = var.tags
}

module "eks" {
  source = "../../../modules/aws/eks"

  name                         = var.name
  kubernetes_version           = "1.36"
  subnet_ids                   = module.network.private_subnet_ids
  platform_admin_principal_arn = var.platform_admin_principal_arn
  kms_key_arn                  = module.data_kms.key_arn
  endpoint_public_access       = false
  node_instance_types          = ["m7i.large"]
  node_min_size                = 2
  node_desired_size            = 2
  node_max_size                = 6
  tags                         = var.tags
}

data "aws_iam_policy_document" "spark_lakehouse" {
  statement {
    sid = "ListLakehouseBucket"
    actions = [
      "s3:GetBucketLocation",
      "s3:ListBucket",
    ]
    resources = [module.lakehouse_storage.bucket_arn]
  }

  statement {
    sid = "ReadWriteLakehouseObjects"
    actions = [
      "s3:AbortMultipartUpload",
      "s3:DeleteObject",
      "s3:GetObject",
      "s3:ListMultipartUploadParts",
      "s3:PutObject",
    ]
    resources = ["${module.lakehouse_storage.bucket_arn}/*"]
  }

  statement {
    sid = "UseLakehouseKey"
    actions = [
      "kms:Decrypt",
      "kms:Encrypt",
      "kms:GenerateDataKey",
    ]
    resources = [module.data_kms.key_arn]
  }
}

module "spark_workload_identity" {
  source = "../../../modules/aws/workload-identity"

  name            = "${var.name}-spark"
  cluster_name    = module.eks.cluster_name
  namespace       = "odp-data"
  service_account = "spark"
  policy_json     = data.aws_iam_policy_document.spark_lakehouse.json
  tags            = var.tags
}
