# hado[o]pi[g]

![Elephant honey](https://raw.githubusercontent.com/masters-info-nantes/hadoopig/master/hadoopig.png)

## Hadoop

### Prerequisites

- correctly installed Hadoop
- define a variable $HADOOP_ROOT containing path to hadoop installation
```
export HADOOP_ROOT="/path/to/hadoop-2.7.1"
```
- make sure $HADOOP_CLASSPATH is set
```
export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar
```

### Compile and run

```
cd hadoop
chmod 777 run.sh
./run.sh
```


## Pig

### Prerequisites

- correctly installed Pig Latin
- define a variable $PIG_ROOT containing path to pig installation
```
export PIG_ROOT="/path/to/pig-0.15.0"
```

## Run script

```
cd pig
chmod 777 run.sh
./run.sh
```
