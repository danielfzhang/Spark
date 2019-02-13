# Set Similarity Join Using Spark+Scala on AWS

Given a threshold, the program outputs all pairs of record with similarity greater or equal to the threshold.

### Jaccard similarity:
`sim(C1, C2) = |C1∩C2| / |C1∪C2|`

### Input Parameters:
`inputFile outputFolder threshold`

### Format of Input Files:
| Index   |      Attributes |
|----------|:-------------:|
|0|today is a good day|
|1|good morning|
|2|have a nice day|
|...|....|

### Optimizations
* PPjoin
* Similarity Will Only Be Computed Once For Same Pairs
* Using Equivalent Transformations But had Higher Performance

### 算子优化
1.使用reduceByKey/aggregateByKey替代groupByKey
 reduceByKey/aggregateByKey底层使用combinerByKey实现，会在map端进行局部聚合；groupByKey不会

2.使用mapPartitions替代普通map
mapPartitions类的算子，一次函数调用会处理一个partition所有的数据，而不是一次函数调用处理一条，性能相对来说会高一些。但是有的时候，使用mapPartitions会出现OOM（内存溢出）的问题。因为单次函数调用就要处理掉一个partition所有的数据，如果内存不够，垃圾回收时是无法回收掉太多对象的，很可能出现OOM异常。所以使用这类操作时要慎重！
