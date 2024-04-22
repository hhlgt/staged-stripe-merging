# limit bandwidth
sh exp.sh 3
# run datanodes and proxies
sh exp.sh 1
# run coordinator
./project/cmake/build/run_coordinator > res/coordinator_exp5_15_2.txt &