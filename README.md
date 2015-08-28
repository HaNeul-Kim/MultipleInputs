# MultipleInputs

## 사용법

먼저 resources/first, resources/second, resources/third 파일을 hdfs 에 올린다

mvn clean install
hadoop jar MultipleInputs-1.0-SNAPSHOT.jar multipleInputs <first> <second> <third> <OUTPUT>
