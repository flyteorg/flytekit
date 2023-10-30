#!/usr/bin/env sh

for _ in $(seq 1 200000); do
  echo "This is an error message" >&2
done

echo "This is the output of the program"
