#include <ProgressMonitor.h>
#include <Utils.h>

namespace cirrus {

ProgressMonitor::ProgressMonitor(const std::string& redis_ip, int redis_port) {
  redis_con = redis_connect(redis_ip.c_str(), redis_port);
}

void ProgressMonitor::increment_batches(int* prev_batch) {
  redis_increment_counter(redis_con, PROGRESS_COUNTER, prev_batch);
}

int ProgressMonitor::get_number_batches() {
  char* nb = redis_get(redis_con, PROGRESS_COUNTER);
  if (nb == nullptr) {
    throw std::runtime_error("Error getting number of batches");
  }

  int ret = string_to<int>(nb);
  free(nb);
  return ret;
}

} // namespace cirrus

