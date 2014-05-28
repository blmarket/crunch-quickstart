Crunch-quickstart
-----------------

Fastest way to start big data application using

  * scala
  * apache crunch

## HOWTO

### Hadoop-only application

```
$ mvn package
$ hadoop jar [jarPath] [className]
```

### hbase application

```
$ mvn package
$ export HBASE_CLASSPATH=target/{generated-jar-file}
$ hbase [className]
```

### IDE integration

Coming soon for IntelliJ IDE!
