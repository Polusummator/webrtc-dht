variable "yc_token" {
  type        = string
  sensitive   = true
}

variable "yc_cloud_id" {
  type        = string
}

variable "yc_folder_id" {
  type        = string
}

variable "zone" {
  type        = string
  default     = "ru-central1-a"
}

variable "node_count" {
  type        = number
  default     = 5
}

variable "ssh_public_key_path" {
  type        = string
  default     = "~/.ssh/id_rsa.pub"
}

variable "ssh_private_key_path" {
  type        = string
  default     = "~/.ssh/id_rsa"
}

variable "ssh_user" {
  type        = string
  default     = "ubuntu"
}

variable "image_family" {
  type        = string
  default     = "ubuntu-2204-lts"
}

variable "disk_size" {
  type        = number
  default     = 8
}

variable "core_fraction" {
  type        = number
  default     = 100
}

variable "bootstrap_cores" {
  type    = number
  default = 2
}

variable "bootstrap_memory" {
  type    = number
  default = 2
}

variable "node_cores" {
  type    = number
  default = 2
}

variable "node_memory" {
  type    = number
  default = 2
}

variable "signal_server_cores" {
  type    = number
  default = 2
}

variable "signal_server_memory" {
  type    = number
  default = 2
}

