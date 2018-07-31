#include <Tasks.h>
#include <iostream>
#include "config.h"
#include "PSSparseServerInterface.h"

namespace cirrus {

#if WAIT_ENABLED
void MLTask::wait_for_start(int index, int nworkers) {
  std::cout << "Waiting for all workers to start (redis). index: " << index
    << std::endl;
  std::cout << "Setting start flag. id: " << index
    << std::endl;

  PSSparseServerInterface psi(ps_ip, ps_port);

  psi.set_status(index, 1);

  int num_waiting_tasks = WORKERS_BASE + nworkers;
  while (1) {
    int i = 1;
    for (; i < num_waiting_tasks; ++i) {
      uint32_t is_done = psi.get_status(i);
      if (i != 1 && !is_done)
        break;
    }
    if (i == num_waiting_tasks) {
      break;
    } else {
      std::cout << "Worker " << i << " not done" << std::endl;
    }
  }
  std::cout << "Worker " << index << " done waiting: " << std::endl;
}
#else
void MLTask::wait_for_start(int index, int nworkers) {}
#endif

} // namespace cirrus

