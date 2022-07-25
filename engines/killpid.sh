#!/usr/bin/env bash

pids=$(pgrep -f "fio ../examples*")

for pid in ${pids[@]}; do
	kill -9 $pid
done

echo "done"