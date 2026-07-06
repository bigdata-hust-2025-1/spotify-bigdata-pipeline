# azure_iac/

Terraform for the cloud footprint: an AKS cluster and a Data Lake Gen2 storage
account.

| File | Role |
| :--- | :--- |
| `versions.tf` | Pins `terraform >= 1.3` and `azurerm ~> 4.0` (reproducible plans). |
| `variables.tf` | Inputs — region, node-pool sizing/autoscaling, `storage_replication_type` (validated, default **ZRS**), retention window, tags. |
| `main.tf` | Resource group, AKS (with cluster-autoscaling), and the Data Lake storage account (blob **versioning** + delete/container retention). |

```bash
terraform init
terraform plan   # override defaults with -var or a *.tfvars file
terraform apply
```

DR/replication rationale (LRS vs ZRS vs GRS) and recovery procedures are in
`docs/DR_AND_SCALING.md`.
