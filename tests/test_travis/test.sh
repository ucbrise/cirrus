#!/bin/bash

for cmd in "timeout 120 ./tests/test_travis/test_ps" "timeout 100 ./tests/test_travis/worker" "timeout 100 ./tests/test_travis/worker" "./tests/test_travis/error"; do
  if eval ${cmd} & sleep 5;then
    echo "success"
  else
    exit 1
  fi
done
