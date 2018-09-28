# Social_Network_Analysis

**Execution:** 

spark-2.2.1-bin-hadoop2.7\bin\spark-submit Tanay_Shankar_Community.py ratings.csv 

spark-2.2.1-bin-hadoop2.7\bin\spark-submit Tanay_Shankar_Betweenness.py ratings.csv 

spark-2.2.1-bin-hadoop2.7\bin\spark-submit Tanay_Shankar_Community_Library.py ratings.csv 
 
**Results:** 
 
*Community Betweenness*

| Method used  Execution Time | Communities | Execution Time |
| ----------------------------|------------|-----------|
| Maximum Modularity (GN) | 460.33 | 224 | 44.99 |
| Girvan Newman (Library) | 353.82 | 4 |
 
