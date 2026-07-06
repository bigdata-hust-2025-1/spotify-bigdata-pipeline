# Pin Terraform + provider versions for reproducible plans (previously
# unpinned, so `terraform init` could pull a breaking provider major and change
# attribute names, e.g. the 3.x->4.x AKS `auto_scaling_enabled` rename).
terraform {
  required_version = ">= 1.3.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
  }
}
