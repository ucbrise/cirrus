#!/bin/bash

for cmd in "timeout 60 ./tests/test_travis/test_ps" "timeout 50 ./tests/test_travis/worker" "timeout 50 ./tests/test_travis/worker" "./tests/test_travis/error"; do
  eval ${cmd} &
  sleep 5
done
