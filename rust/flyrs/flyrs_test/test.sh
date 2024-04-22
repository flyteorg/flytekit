export PYTHONPATH=`pwd`:$PYTHONPATH

TESTDATA_PATH=`pwd`/testdata

../target/debug/flyrs --inputs ${TESTDATA_PATH}/inputs.pb --output-prefix ${TESTDATA_PATH} --raw-output-data-prefix ${TESTDATA_PATH} --dynamic-addl-distro file://${TESTDATA_PATH}/schedule.tar.gz --dynamic-dest-dir . --resolver "flytekit.core.python_auto_container.default_task_resolver" -- task-module schedule task-name say_hello

cmp testdata/outputs.pb testdata/expected_outputs.pb || echo -e "----------- Outputs file comparision failed! ----------"
