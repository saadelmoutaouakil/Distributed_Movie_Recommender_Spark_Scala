#!/usr/bin/env bash
# If your default java install does not work, explicitly 
# provide the path to the JDK 1.8 installation. On OSX
# with homebrew:
# export JAVA_HOME=/usr/local/Cellar/openjdk@8/1.8.0+282; ./run.sh
export JAVA_OPTS="-Xmx8G";
RUN=./logs/run-$(date "+%Y-%m-%d-%H:%M:%S")-$(hostname)
mkdir -p $RUN
LOGS=$RUN/log.txt
source ./config.sh 
echo "------------------- OPTIMIZING    ---------------------" >> $LOGS
sbt "runMain scaling.Optimizing --train $ML100Ku2base --test $ML100Ku2test --json $RUN/optimizing-100k.json --users 943 --movies 1682 --master local[1]" 2>&1 >>$LOGS
echo "------------------- DISTRIBUTED EXACT ---------------------" >> $LOGS
sbt "runMain distributed.Exact --train $ML100Ku2base --test $ML100Ku2test --json $RUN/exact-100k-4.json --k 10 --master local[4] --users 943 --movies 1682" 2>&1 >>$LOGS
sbt "runMain distributed.Exact --train $ML1Mrbtrain --test $ML1Mrbtest --separator :: --json $RUN/exact-1m-4.json --k 300 --master local[4] --users 6040 --movies 3952" 2>&1 >>$LOGS
echo "------------------- DISTRIBUTED APPROXIMATE ---------------------" >> $LOGS
sbt "runMain distributed.Approximate --train $ML100Ku2base --test $ML100Ku2test --json $RUN/approximate-100k-4-k10-r2.json --k 10 --master local[4] --users 943 --movies 1682 --partitions 10 --replication 2" 2>&1 >>$LOGS;
for R in 1 2 3 4 6 8; do
    sbt "runMain distributed.Approximate --train $ML100Ku2base --test $ML100Ku2test --json $RUN/approximate-100k-4-k300-r$R.json --k 300 --master local[4] --users 943 --movies 1682 --partitions 10 --replication $R" 2>&1 >>$LOGS;
done
sbt "runMain distributed.Approximate --train $ML1Mrbtrain --test $ML1Mrbtest --separator :: --json $RUN/approximate-1m-4.json --k 300 --master local[4] --users 6040 --movies 3952 --partitions 8 --replication 1" 2>&1 >>$LOGS
echo "------------------- ECONOMICS -----------------------------------" >> $LOGS
sbt "runMain economics.Economics --json $RUN/economics.json" 2>&1 >>$LOGS
