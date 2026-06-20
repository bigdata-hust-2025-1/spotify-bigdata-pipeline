# Định cấu hình nhà cung cấp Azure
provider "azurerm" {
  features {}
}

# Tạo Resource Group
resource "azurerm_resource_group" "spotify_rg" {
  name     = "spotify-bigdata-pipeline-rg"
  location = "East US"
}

# Tạo Azure Kubernetes Service (AKS) cho việc triển khai platform
resource "azurerm_kubernetes_cluster" "aks_cluster" {
  name                = "spotify-aks-cluster"
  location            = azurerm_resource_group.spotify_rg.location
  resource_group_name = azurerm_resource_group.spotify_rg.name
  dns_prefix          = "spotifyaks"

  default_node_pool {
    name       = "default"
    node_count = 3
    vm_size    = "Standard_DS2_v2" # Kích thước máy ảo
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
  account_replication_type = "LRS"
  is_hns_enabled           = true # Kích hoạt Hierarchical Namespace cho Data Lake Gen2
}
