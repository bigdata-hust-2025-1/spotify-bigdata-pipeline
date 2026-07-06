# Input variables for the Spotify big-data platform infrastructure.
# Defaults are dev-sane; production overrides go in a *.tfvars file.

variable "location" {
  description = "Azure region for all resources."
  type        = string
  default     = "East US"
}

variable "resource_group_name" {
  description = "Name of the resource group that holds the platform."
  type        = string
  default     = "spotify-bigdata-pipeline-rg"
}

# --- AKS node pool -----------------------------------------------------------

variable "node_vm_size" {
  description = "VM size for the default AKS node pool."
  type        = string
  default     = "Standard_DS2_v2"
}

variable "node_count" {
  description = "Fixed node count when autoscaling is disabled."
  type        = number
  default     = 3
}

variable "enable_auto_scaling" {
  description = "Enable AKS cluster-autoscaler on the default node pool."
  type        = bool
  default     = true
}

variable "node_min_count" {
  description = "Minimum node count when autoscaling is enabled."
  type        = number
  default     = 3
}

variable "node_max_count" {
  description = "Maximum node count when autoscaling is enabled (scaling lever)."
  type        = number
  default     = 6
}

# --- Data Lake storage -------------------------------------------------------

variable "storage_replication_type" {
  description = <<-EOT
    Replication tier for the Data Lake storage account. Defaults to ZRS
    (zone-redundant, survives a single-AZ loss in-region). Use GRS/RA-GRS for
    cross-region disaster recovery. LRS is single-AZ and NOT recommended for
    anything but throwaway dev — see docs/DR_AND_SCALING.md.
  EOT
  type        = string
  default     = "ZRS"

  validation {
    condition     = contains(["LRS", "ZRS", "GRS", "RAGRS", "GZRS", "RAGZRS"], var.storage_replication_type)
    error_message = "storage_replication_type must be one of LRS, ZRS, GRS, RAGRS, GZRS, RAGZRS."
  }
}

variable "blob_retention_days" {
  description = "Days to retain soft-deleted blobs / previous versions (recovery window)."
  type        = number
  default     = 30
}

variable "tags" {
  description = "Tags applied to every resource."
  type        = map(string)
  default = {
    project     = "spotify-bigdata-pipeline"
    environment = "dev"
    managed-by  = "terraform"
  }
}
