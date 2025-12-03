variable "project_id" {
  type        = string
  description = "The identifier of the GCP project. Can be CLI configured"
}

variable "default_region" {
  type        = string
  description = "The default region to use for resources that require one. Defaults to europe-west3"
  default     = "europe-west3"
}