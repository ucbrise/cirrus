#include <PSSparseServerInterface.h>

using namespace cirrus;

#define HIGH_TIMEOUT (1000)

int main() {
  std::unique_ptr<PSSparseServerInterface> psi =
      std::make_unique<PSSparseServerInterface>("127.0.0.1", 1337);
  psi->connect();

  uint32_t registered = psi->register_task(0, HIGH_TIMEOUT);

  if (registered != 0) {
    throw std::runtime_error("Expected 0");
  }

  std::cout << "Deregister task" << std::endl;
  registered = psi->deregister_task(0);
  if (registered != 0) {
    throw std::runtime_error("Expected 1");
  }

  registered = psi->register_task(0, HIGH_TIMEOUT);
  if (registered != 1) {
    throw std::runtime_error("Expected 1");
  }

  registered = psi->deregister_task(0);
  if (registered != 1) {
    throw std::runtime_error("Expected 1");
  }

  // test that timeout works

  // register taks with id 1 and timeout 0 seconds
  registered = psi->register_task(1, 0);
  if (registered != 0) {
    throw std::runtime_error("Expected 1");
  }

  sleep(5);
  // after 5 seconds task should have been deregistered

  registered = psi->deregister_task(1);
  if (registered != 1) {
    throw std::runtime_error("Expected 1");
  }

  registered = psi->deregister_task(2);
  if (registered != 2) {
    throw std::runtime_error("Expected 2");
  }

  return 0;
}
