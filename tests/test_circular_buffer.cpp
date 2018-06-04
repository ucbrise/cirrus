#include "CircularBuffer.h"

CircularBuffer<int> cb(100);

int main() {
  cb.add(1);

  assert(cb.size() == 1);
  int ret = cb.pop();
  assert(ret == 1);

  cb.add(3);
  assert(cb.size() == 1);
  cb.add(5);
  assert(cb.size() == 2);

  return 0;
}
