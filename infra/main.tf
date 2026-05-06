terraform {
  required_providers {
    yandex = {
      source  = "yandex-cloud/yandex"
      version = "~> 0.100"
    }
  }
}

provider "yandex" {
  token     = var.yc_token
  cloud_id  = var.yc_cloud_id
  folder_id = var.yc_folder_id
  zone      = var.zone
}

data "yandex_compute_image" "ubuntu" {
  family = var.image_family
}

resource "yandex_vpc_network" "bench" {
  name = "webrtc-dht-bench"
}

resource "yandex_vpc_gateway" "nat" {
  name = "webrtc-dht-nat-gateway"
  shared_egress_gateway {}
}

resource "yandex_vpc_route_table" "nat" {
  name       = "webrtc-dht-nat-rt"
  network_id = yandex_vpc_network.bench.id

  static_route {
    destination_prefix = "0.0.0.0/0"
    gateway_id         = yandex_vpc_gateway.nat.id
  }
}

resource "yandex_vpc_subnet" "bench" {
  name           = "webrtc-dht-bench-subnet"
  zone           = var.zone
  network_id     = yandex_vpc_network.bench.id
  v4_cidr_blocks = ["10.10.0.0/16"]
  route_table_id = yandex_vpc_route_table.nat.id
}

resource "yandex_vpc_security_group" "bench" {
  name       = "webrtc-dht-bench-sg"
  network_id = yandex_vpc_network.bench.id

  ingress {
    protocol       = "TCP"
    description    = "SSH"
    v4_cidr_blocks = ["0.0.0.0/0"]
    port           = 22
  }

  ingress {
    protocol       = "ANY"
    description    = "All internal"
    v4_cidr_blocks = ["10.10.0.0/16"]
    from_port      = 0
    to_port        = 65535
  }

  ingress {
    protocol       = "TCP"
    description    = "Signal server HTTP"
    v4_cidr_blocks = ["0.0.0.0/0"]
    port           = 9000
  }

  ingress {
    protocol       = "TCP"
    description    = "Coordinator HTTP"
    v4_cidr_blocks = ["0.0.0.0/0"]
    port           = 9100
  }

  egress {
    protocol       = "ANY"
    v4_cidr_blocks = ["0.0.0.0/0"]
    from_port      = 0
    to_port        = 65535
  }
}

resource "yandex_compute_instance" "signal_server" {
  name        = "signal-server"
  platform_id = "standard-v1"
  zone        = var.zone

  resources {
    cores         = var.signal_server_cores
    memory        = var.signal_server_memory
    core_fraction = var.core_fraction
  }

  boot_disk {
    initialize_params {
      image_id = data.yandex_compute_image.ubuntu.id
      size     = var.disk_size
    }
  }

  network_interface {
    subnet_id          = yandex_vpc_subnet.bench.id
    security_group_ids = [yandex_vpc_security_group.bench.id]
    nat                = true
  }

  metadata = {
    ssh-keys = "${var.ssh_user}:${file(var.ssh_public_key_path)}"
  }
}

resource "yandex_compute_instance" "bootstrap" {
  name        = "dht-bootstrap"
  platform_id = "standard-v1"
  zone        = var.zone

  resources {
    cores         = var.bootstrap_cores
    memory        = var.bootstrap_memory
    core_fraction = var.core_fraction
  }

  boot_disk {
    initialize_params {
      image_id = data.yandex_compute_image.ubuntu.id
      size     = var.disk_size
    }
  }

  network_interface {
    subnet_id          = yandex_vpc_subnet.bench.id
    security_group_ids = [yandex_vpc_security_group.bench.id]
    nat                = true
  }

  metadata = {
    ssh-keys = "${var.ssh_user}:${file(var.ssh_public_key_path)}"
  }
}

resource "yandex_compute_instance" "node" {
  count       = var.node_count
  name        = "dht-node-${count.index}"
  platform_id = "standard-v1"
  zone        = var.zone

  resources {
    cores         = var.node_cores
    memory        = var.node_memory
    core_fraction = var.core_fraction
  }

  boot_disk {
    initialize_params {
      image_id = data.yandex_compute_image.ubuntu.id
      size     = var.disk_size
    }
  }

  network_interface {
    subnet_id          = yandex_vpc_subnet.bench.id
    security_group_ids = [yandex_vpc_security_group.bench.id]
  }

  metadata = {
    ssh-keys = "${var.ssh_user}:${file(var.ssh_public_key_path)}"
  }
}

