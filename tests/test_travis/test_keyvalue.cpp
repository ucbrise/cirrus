#include <PSSparseServerInterface.h>

using namespace cirrus;

#define VALUE_SIZE (1000)

int main() {
  std::unique_ptr<PSSparseServerInterface> psi =
      std::make_unique<PSSparseServerInterface>("127.0.0.1", 1337);
  psi->connect();

  char value[VALUE_SIZE];
  for (int i = 0; i < VALUE_SIZE; ++i) {
    value[i] = i;
  }

  psi->set_value("key", value, sizeof(value));

  std::pair<std::shared_ptr<char>, uint32_t> ret = psi->get_value("key");

  if (ret.second != sizeof(value)) {
    throw std::runtime_error("Wrong size");
  }
  if (memcmp(&value, ret.first.get(), sizeof(value))) {
    throw std::runtime_error("Wrong value");
  }

  return 0;
}
