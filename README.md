# SparkIsax
the fastest distributed algorithm for building the timeseries index
## Installation
### prerequisite
1.copy spark libs and hbase libs into libs
2.intellij with scala plugin
3.scala 10.4+
4.Java1.7 +
5.package the project into a jar(named SparkIsax.jar)
because of the size limit on the github, i didn't push my libs and jars.

##Usage
spark-submit --master spark://master:7077 SparkIsax.jar  tableName  inputPath MaximumDepth 

