output "vpc_id" {
  value = module.network.vpc_id
}

output "eks_cluster_name" {
  value = module.eks.cluster_name
}

output "lakehouse_bucket_name" {
  value = module.lakehouse_storage.bucket_name
}

output "lakehouse_kms_key_arn" {
  value = module.data_kms.key_arn
}

output "spark_workload_role_arn" {
  value = module.spark_workload_identity.role_arn
}
