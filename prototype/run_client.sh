COR_IP='0.0.0.0'
STRIPE_NUM=32
KK=6
LL=2
G_M=2
X1=2
X2=2
X3=0
# VALUE_SIZE=49152   #48MB, 8MB, (6, 2, 2)
# VALUE_SIZE=98304   #96MB, 16MB, (6, 2, 2)
# VALUE_SIZE=196608  #192MB, 32MB, (6, 2, 2)
VALUE_SIZE=393216  #384MB, 64MB, (6, 2, 2)
# VALUE_SIZE=786432  #768MB, 128MB, (6, 2, 2)
# VALUE_SIZE=262144  #256MB, 64MB, (4, 2, 2)

# run client
./project/cmake/build/run_client false Azure_LRC Optimal Ran ${KK} ${LL} ${G_M} ${STRIPE_NUM} ${X1} ${X2} ${X3} ${VALUE_SIZE} ${COR_IP}
./project/cmake/build/run_client true Azure_LRC Optimal Ran ${KK} ${LL} ${G_M} ${STRIPE_NUM} ${X1} ${X2} ${X3} ${VALUE_SIZE} ${COR_IP}
./project/cmake/build/run_client true Azure_LRC Optimal DIS ${KK} ${LL} ${G_M} ${STRIPE_NUM} ${X1} ${X2} ${X3} ${VALUE_SIZE} ${COR_IP}
./project/cmake/build/run_client true Azure_LRC Optimal AGG ${KK} ${LL} ${G_M} ${STRIPE_NUM} ${X1} ${X2} ${X3} ${VALUE_SIZE} ${COR_IP}
./project/cmake/build/run_client true Azure_LRC Optimal OPT ${KK} ${LL} ${G_M} ${STRIPE_NUM} ${X1} ${X2} ${X3} ${VALUE_SIZE} ${COR_IP}

# unlimit bandwidth
sh exp.sh 4
# kill datanodes and proxies
sh exp.sh 0
# kill coordinator
pkill -9 run_coordinator

