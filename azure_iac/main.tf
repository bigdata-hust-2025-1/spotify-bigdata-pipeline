# Định cấu hình nhà cung cấp Azure
provider "azurerm" {
  features {
    # Let `terraform destroy` remove the RG even if it still has resources.
    resource_group {
      prevent_deletion_if_contains_resources = false
    }
  }
}

# Tạo Resource Group
resource "azurerm_resource_group" "spotify_rg" {
  name     = var.resource_group_name
  location = var.location
  tags     = var.tags
}

# Tạo Azure Kubernetes Service (AKS) cho việc triển khai platform
resource "azurerm_kubernetes_cluster" "aks_cluster" {
  name                = "spotify-aks-cluster"
  location            = azurerm_resource_group.spotify_rg.location
  resource_group_name = azurerm_resource_group.spotify_rg.name
  dns_prefix          = "spotifyaks"
  tags                = var.tags

  default_node_pool {
    name    = "default"
    vm_size = var.node_vm_size

    # Scaling lever: the cluster-autoscaler grows/shrinks the pool between
    # min/max as Spark/Airflow pods queue. When autoscaling is disabled we pin a
    # fixed node_count instead (see docs/DR_AND_SCALING.md).
    auto_scaling_enabled = var.enable_auto_scaling
    node_count           = var.enable_auto_scaling ? null : var.node_count
    min_count            = var.enable_auto_scaling ? var.node_min_count : null
    max_count            = var.enable_auto_scaling ? var.node_max_count : null
  }

  identity {
    type = "SystemAssigned"
  }
}

# Azure Data Lake Storage Gen2 (Thay thế hoặc bổ trợ cho MinIO trên Cloud)
resource "azurerm_storage_account" "datalake" {
  name                     = "spotifydatalakegen2"
  resource_group_name      = azurerm_resource_group.spotify_rg.name
  location                 = azurerm_resource_group.spotify_rg.location
  account_tier             = "Standard"
  account_replication_type = var.storage_replication_type # replicated by default (ZRS)
  is_hns_enabled           = true                          # Hierarchical Namespace cho Data Lake Gen2
  tags                     = var.tags

  # Object versioning + soft-delete give point-in-time recovery from accidental
  # overwrite/delete (the cloud equivalent of MinIO bucket versioning). Combined
  # with Iceberg snapshots this is the storage-layer half of the DR story.
  blob_properties {
    versioning_enabled = true

    delete_retention_policy {
      days = var.blob_retention_days
    }

    container_delete_retention_policy {
      days = var.blob_retention_days
    }
  }
}

output "aks_cluster_name" {
  description = "Name of the provisioned AKS cluster."
  value       = azurerm_kubernetes_cluster.aks_cluster.name
}

output "datalake_storage_account" {
  description = "Data Lake Gen2 storage account name."
  value       = azurerm_storage_account.datalake.name
}
