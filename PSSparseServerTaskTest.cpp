#include <poll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <unistd.h>
#include <stdexcept>
#include <cstring>
#include <iostream>
#include "ModelGradient.h"
#include "Utils.h"

void send_message(int sock) {
  uint32_t operation = 1;
  int ret = send(sock, &operation, sizeof(uint32_t), 0);
  //std::cout << "Sent ret: " << ret << std::endl;

  cirrus::LRSparseGradient gradient(100);
  uint32_t size = gradient.getSerializedSize();
  //std::cout << "Sending grad size: " << size << std::endl;
  ret = send(sock, &size, sizeof(uint32_t), 0);
  //std::cout << "Sent ret: " << ret << std::endl;
  char data[size] = {0};
  gradient.serialize(data);
  ret = send(sock, data, size, 0);
  if (ret == -1) {
    throw std::runtime_error("Error sending message");
  }
}

int main() {
  int sock;
  if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
    throw std::runtime_error("Error when creating socket.");
  }   
  int opt = 1;
  if (setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt))) {
    throw std::runtime_error("Error setting socket options.");
  }   

  std::string address = "127.0.0.1";
  struct sockaddr_in serv_addr;
  serv_addr.sin_family = AF_INET;
  if (inet_pton(AF_INET, address.c_str(), &serv_addr.sin_addr) != 1) {
    throw std::runtime_error("Address family invalid or invalid "
        "IP address passed in");
  }   
  // Convert port from string to int
  std::string port_string = "1337";
  int port = stoi(port_string, nullptr);

  // Save the port in the info
  serv_addr.sin_port = htons(port);
  std::memset(serv_addr.sin_zero, 0, sizeof(serv_addr.sin_zero));

  // Connect to the server
  if (::connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
    throw std::runtime_error(
        "Client could not connect to server."
        " Address: " + address + " port: " + port_string);
  }

  auto before = cirrus::get_time_us();
  for (uint32_t i = 0; i < 10000; ++i) {
    send_message(sock);
  }
  auto after = cirrus::get_time_us();
  auto elapsed_us = (after - before);
  std::cout << "ops/sec: " << (10000.0 / elapsed_us * 1000 * 1000) << std::endl;

  return 0;
}
