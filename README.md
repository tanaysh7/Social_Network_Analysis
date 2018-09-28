# Social_Network_Analysis

Community detection making cuts at links with highest betweenness from [MovieLens](http://files.grouplens.org/datasets/movielens/ml-latest-small-README.html) dataset.
Implemented Girvan Newman algorithm to find optimum cuts in the graph.

**Execution:** 

spark-2.2.1-bin-hadoop2.7\bin\spark-submit Tanay_Shankar_Community.py ratings.csv 

spark-2.2.1-bin-hadoop2.7\bin\spark-submit Tanay_Shankar_Betweenness.py ratings.csv 

spark-2.2.1-bin-hadoop2.7\bin\spark-submit Tanay_Shankar_Community_Library.py ratings.csv 
 
**Results:** 
 
*Community Betweenness*

| Method used  | Execution Time | Communities |
| -------------|------------|-------------|
| Maximum Modularity (GN) | 460.33 | 224 |
| Girvan Newman (Library) | 353.82 | 4 |
 
