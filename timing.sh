#!/usr/bin/env bash
# If your default java install does not work, explicitly 
# provide the path to the JDK 1.8 installation. On OSX
# with homebrew:
# export JAVA_HOME=/usr/local/Cellar/openjdk@8/1.8.0+282; ./run.sh
export JAVA_OPTS="-Xmx8G";
RUN=./logs/timing-$(date "+%Y-%m-%d-%H:%M:%S")-$(hostname)
mkdir -p $RUN
LOGS=$RUN/log.txt
source ./config.sh 
echo "------------------- OPTIMIZING    ---------------------" >> $LOGS
sbt "runMain scaling.Optimizing --train $ML100Ku2base --test $ML100Ku2test --json $RUN/optimized-100k.json --users 943 --movies 1682 --num_measurements 3" 2>&1 >>$LOGS
echo "------------------- DISTRIBUTED EXACT ---------------------" >> $LOGS
for W in 1 2 4; do
    sbt "runMain distributed.Exact --train $ML1Mrbtrain --test $ML1Mrbtest --separator :: --json $RUN/exact-1m-$W.json --k 300 --master local[$W] --users 6040 --movies 3952 --num_measurements 3" 2>&1 >>$LOGS;
done
echo "------------------- APPROXIMATE EXACT ---------------------" >> $LOGS
for W in 1 2 4; do
    sbt "runMain distributed.Approximate --train $ML1Mrbtrain --test $ML1Mrbtest --separator :: --json $RUN/approximate-1m-$W.json --k 300 --master local[$W] --users 6040 --movies 3952 --num_measurements 3" 2>&1 >>$LOGS;
done
