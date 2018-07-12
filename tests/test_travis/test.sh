#!/bin/bash

for cmd in "timeout 30 ./tests/test_travis/test_ps" "timeout 20 ./tests/test_travis/worker" "timeout 20 ./tests/test_travis/worker"; do
  eval ${cmd} &
  sleep 1
done
