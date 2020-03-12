#!/bin/bash

BIN_DIR=$(dirname $(readlink -f $0))

print_info() {
    echo "run_tests.sh [connection-str]"
}

TESTS=(
    "com.github.phantomthief.zookeeper.EphemeralTest"
    "com.github.phantomthief.zookeeper.TransactionTest"
    "com.github.phantomthief.zookeeper.ZkBasedTreeNodeResourceTest"
    "com.github.phantomthief.zookeeper.ZkBaseNodeCloseTest"
    "com.github.phantomthief.zookeeper.ZkBaseNodeEmptyTest"
    "com.github.phantomthief.zookeeper.ZkBaseNodeInterruptTest"
    "com.github.phantomthief.zookeeper.ZkBaseNodeRefreshTest"
    "com.github.phantomthief.zookeeper.ZkBaseNodeResourceTest"
    "com.github.phantomthief.zookeeper.ZkBaseTreeCloseTest"
    "com.github.phantomthief.zookeeper.ZkBaseTreeInterruptTest"
    "com.github.phantomthief.zookeeper.ZkBaseTreeNodeExceptionTest"
    "com.github.phantomthief.zookeeper.test.ZkNodeTest"
    "com.github.phantomthief.zookeeper.util.ZkUtilsTest"
)

JAVA_EXE="java"

if [ "x$JAVA_HOME" != "x" ]; then
    JAVA_EXE=$JAVA_HOME/bin/java
fi

TEST_PROP="${TEST_PROP}"
if [ "x$1" != "x" ]; then
    TEST_PROP="${TEST_PROP} -Dzk.other.connectionStr=${1}"
fi

ARGS=""

for t in ${TESTS[*]}
do
    ARGS="$ARGS -c $t"
done

LAUNCHER="org.junit.platform.console.ConsoleLauncher"

print_info

$JAVA_EXE -cp $(echo lib/*.jar | tr ' ' ':') $TEST_PROP $LAUNCHER $ARGS