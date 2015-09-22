# Execution Instructions

This explains the build and run instructions for Samoa on Apache Apex (http://apex.apache.org/)

## Build

Simply clone the repository and and create SAMOA with Apex package.
```bash
git clone http://git.apache.org/incubator-samoa.git
cd incubator-samoa
mvn -Papex package
```

The deployable jar will be present in `target/SAMOA-Apex-0.4.0-incubating-SNAPSHOT.jar`.

## Running Samoa Algorithms on Apex in Local mode
- Edit samoa-apex.properties. Make sure `samoa.apex.mode` is set to `local`
- Run the deployable jar from the top level Samoa directory using the parameters for the algorithm. For example, for running the VHT classifier, run: `bin/samoa apex target/SAMOA-Apex-0.4.0-incubating-SNAPSHOT.jar "PrequentialEvaluation -d /tmp/dump.csv -i 1000000 -l (classifiers.trees.VerticalHoeffdingTree -p 2) -s (generators.RandomTreeGenerator -c 2 -o 10 -u 10)"` 

## Running Samoa Algorithms on Apex in Cluster mode
- A running Hadoop 2.0 (YARN) cluster is necessary for running Apex in cluster mode.
- Edit samoa-apex.properties. 
 - Make sure `samoa.apex.mode` is set to `cluster`
 - Set the `dt.dfsRootDirectory` parameter to point to a valid HDFS directory
 - Set the `fs.default.name` parameter to point to the name node service of the Hadoop cluster
- Run the deployable jar from the top level Samoa directory using the parameters for the algorithm. For example, for running the VHT classifier, run: `bin/samoa apex target/SAMOA-Apex-0.4.0-incubating-SNAPSHOT.jar "PrequentialEvaluation -d /tmp/dump.csv -i 1000000 -l (classifiers.trees.VerticalHoeffdingTree -p 2) -s (generators.RandomTreeGenerator -c 2 -o 10 -u 10)"` 
