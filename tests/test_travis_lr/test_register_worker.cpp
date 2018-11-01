#include <PSSparseServerInterface.h>

using namespace cirrus;

int main() {
  std::unique_ptr<PSSparseServerInterface> psi =
      std::make_unique<PSSparseServerInterface>("127.0.0.1", 1337);
  psi->connect();

  uint32_t registered = psi->register_task(0, 100);

  if (registered != 0) {
    throw std::runtime_error("Expected 0");
  }

  std::cout << "Deregister task" << std::endl;
  registered = psi->deregister_task(0);
  if (registered != 1) {
    throw std::runtime_error("Expected 1");
  }

  registered = psi->register_task(0, 100);
  if (registered != 1) {
    throw std::runtime_error("Expected 1");
  }

  registered = psi->deregister_task(0);
  if (registered != 1) {
    throw std::runtime_error("Expected 1");
  }

  return 0;
}
