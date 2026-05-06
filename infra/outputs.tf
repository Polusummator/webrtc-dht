output "signal_server_public_ip" {
  value = yandex_compute_instance.signal_server.network_interface[0].nat_ip_address
}

output "signal_server_internal_ip" {
  value = yandex_compute_instance.signal_server.network_interface[0].ip_address
}

output "bootstrap_public_ip" {
  value = yandex_compute_instance.bootstrap.network_interface[0].nat_ip_address
}

output "bootstrap_internal_ip" {
  value = yandex_compute_instance.bootstrap.network_interface[0].ip_address
}

output "node_internal_ips" {
  value = [for n in yandex_compute_instance.node : n.network_interface[0].ip_address]
}

