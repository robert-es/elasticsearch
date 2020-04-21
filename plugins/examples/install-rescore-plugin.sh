#!bin/bash

export GRADLE_HOME=/Users/a123/software/gradle-5.2.1/
export PATH=$GRADLE_HOME/bin:$PATH
rm /Users/a123/workspace/opensource/elasticsearch/plugins/examples/rescore/build/distributions/example-rescore-6.7.0-SNAPSHOT.zip
gradle assemble
rm /Users/a123/workspace/opensource/elasticsearch/distribution/src/main/resources/plugins/example-rescore/*

unzip /Users/a123/workspace/opensource/elasticsearch/plugins/examples/rescore/build/distributions/example-rescore-6.7.0-SNAPSHOT.zip -d /Users/a123/workspace/opensource/elasticsearch/distribution/src/main/resources/plugins/example-rescore/



