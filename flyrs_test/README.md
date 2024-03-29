# flytekit-rust-entrypoint `flyrs`
This is replacement for the entrypoint.py (pyflyte-fast-execute) and others in flytekit with a rust entrypoint


# To run the test

```
time ./test.sh
```

# Things to consider
./test.sh will not build for now, i have commented it out

Also for some reason even if you use virtual env, rust will use the root python interpreter that is the symlinked version in your virtual env. So you will need to start a virtual env, but install flytekit in root.


# Preliminary results

```
running executor
Adding trailing sep to
./test.sh  0.78s user 2.62s system 618% cpu 0.550 total

(flytekit) ➜  flytekit-rust-entrypoint git:(main) ✗ time ./run_python.sh                                                                                                                                                          <aws:open-compute>
x schedule.py

./run_python.sh  2.92s user 7.16s system 373% cpu 2.695 total
```

3.8x speedup - nuts!
