#!/bin/bash

#Usage
#  run_all_plugins.sh $@
#
# This script is intended to be run from the base cookbook_se directory. It will go through all sub-folders in the
# recipes folder and run the 'target' supplied if a Makefile exists in that directory.


DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

shopt -s dotglob
find * -prune -type d | while IFS= read -r d; do
    if [ -f "$d/setup.py" ]; then
        echo "Running your command in $d..."
        cd $d;
        $@;
        cd -;
    fi
done
