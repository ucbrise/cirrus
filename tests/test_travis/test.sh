#!/bin/bash

timeout 60 ./tests/test_travis/test_ps&
sleep 1

timeout 50 ./tests/test_travis/worker&
sleep 1

timeout 50 ./tests/test_travis/worker&
sleep 1
