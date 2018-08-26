#!/bin/bash

for cmd in "timeout 60 ./tests/test_travis_mf/test_ps" "timeout 50 ./tests/test_travis_mf/worker" "timeout 50 ./tests/test_travis_mf/worker"; do
  eval ${cmd} &
  sleep 1
done
