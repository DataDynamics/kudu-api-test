# kudu-api-test
kudu api test

```shell
./mvnw clean package -DskipTests -s .mvn/wrapper/settings.xml
```

```shell
# spark-shell --jars file:///opt/cloudera/parcels/CDH/jars/*kudu*
spark-shell --jars file:///opt/cloudera/parcels/CDH/jars/*kudu*
```

**jars option 에 포함되는 library 의 scala version 과 spark 의 scala version 은 꼭 동일해야 함**
