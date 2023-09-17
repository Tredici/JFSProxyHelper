#!/bin/bash

JAR="CDNDispatcherHelper-1.0-SNAPSHOT-jar-with-dependencies.jar"
JDIR=target/
DIR=$(dirname $0)/$JDIR/$JAR
java -jar $DIR "$@"
