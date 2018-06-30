# Set Similarity Join Using Spark+Scala on AWS

Given a threshold, the program outputs all pairs of record with similarity greater or equal to threshold.

### Jaccard similarity:
`sim(C1, C2) = |C1∩C2| / |C1∪C2|`

### Input Parameters:
`inputFile outputFolder threshold`

### Format of Input Files:
`index element1 element2 element...`

### Optimize the performance with PPjoin and Transformations selection
