#!/usr/bin/env bash
# If your default java install does not work, explicitly 
# provide the path to the JDK 1.8 installation. On OSX
# with homebrew:
# export JAVA_HOME=/usr/local/Cellar/openjdk@8/1.8.0+282; ./test.sh
export JAVA_OPTS="-Xmx8G";
RUN=./logs/test-$(date "+%Y-%m-%d-%H:%M:%S")-$(hostname)
mkdir -p $RUN
LOGS=$RUN/log.txt
sbt "testOnly test.AllTests" 2>&1 >>$LOGS
