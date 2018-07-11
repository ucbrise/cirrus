#!/bin/bash

for cmd in "timeout 120 ./tests/test_travis/test_ps" "timeout 100 ./tests/test_travis/worker" "timeout 100 ./tests/test_travis/worker"; do
  eval ${cmd} & sleep 5
done
