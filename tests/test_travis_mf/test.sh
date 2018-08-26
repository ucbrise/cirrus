#!/bin/bash

for cmd in "timeout 120 ./tests/test_travis_mf/test_ps" "timeout 100 ./tests/test_travis_mf/worker" "timeout 100 ./tests/test_travis_mf/worker"; do
  eval ${cmd} &
  sleep 1
done
