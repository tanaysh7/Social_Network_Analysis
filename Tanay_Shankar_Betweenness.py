import pyspark
import csv
import sys
from pyspark import SparkContext
import collections
from itertools import combinations
import numpy as np
import random
import networkx as nx
import time






sc = SparkContext("local[*]",appName="inf553")
sc.setLogLevel("ERROR")



filename=sys.argv[1]
rdd = sc.textFile(filename) 
rdd = rdd.mapPartitions(lambda x: csv.reader(x))
header = rdd.first() #extract header
data = rdd.filter(lambda row: row != header)   #filter out header

t0 = time.time()

# In[5]:


data = data.map(lambda x:(int(x[0]),int(x[1])))
distmovies=data.map(lambda x: x[1]).distinct().sortBy(lambda x: x).collect()


data=data.groupByKey().map(lambda x: (x[0],list(x[1]))).sortBy(lambda x: x[0])
datas=data.collect()





nodes=[]
for i,j in datas:
    for k,l in datas:
        if i!=k:
            if len(set(j).intersection(l))>8:
                nodes.append((i,k))


# In[224]:


G = nx.Graph()
G.add_edges_from(nodes)



btns = nx.edge_betweenness_centrality(G,normalized=False)

t1 = time.time()

total = t1-t0
print total





with open('Tanay_Shankar_Betweenness.txt', 'wb') as f: 
    for i in sorted(btns):
        f.write(str(i).replace(')',',').replace(' ','')+str(btns[i])+')')
        f.write('\n')
f.close()
