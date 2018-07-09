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
