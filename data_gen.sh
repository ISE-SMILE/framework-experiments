#!/bin/bash

for i in {1..10}; do ./dbgen -vf -s 5 -T L -S $i -C 10; done.
