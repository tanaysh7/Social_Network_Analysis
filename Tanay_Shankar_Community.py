import pyspark
import csv
import sys
from pyspark import SparkContext
import collections
from itertools import combinations
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



data = data.map(lambda x:(int(x[0]),int(x[1])))
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




maxbts=[]
for key,val in sorted(btns.iteritems(), key=lambda (k,v): (v,k),reverse= True):
    maxbts.append(tuple(sorted(key)))


# In[228]:


originalGraph=G.copy()


# In[230]:


degree=dict(originalGraph.degree())
Kij=dict()
for i in list(combinations(degree.iterkeys(),2)):
    Kij[(i[0],i[1])]=degree[i[0]]*degree[i[1]]


# In[231]:


combinedEdges=dict()
for i in maxbts:
    if btns[i] in combinedEdges:
        combinedEdges[btns[i]].append(i)
    else:
        combinedEdges[btns[i]]=[i]


# In[233]:


edgedict=dict()
for i in originalGraph.edges():
    edgedict[i]=1



m=len(edgedict)
MaxM=-1
existingS=0
for key in sorted(combinedEdges.iterkeys(),reverse=True):
    
    maxedge=combinedEdges[key]
    #Remove Edge
    G.remove_edges_from(maxedge)
    #Calculate modularity
    currentS=nx.number_connected_components(G)
    if currentS>existingS:
        existingS=currentS
        Q=float(0)
        components=list(nx.connected_components(G))
        for module in components:
            sumAij=0
            sumKiKj=0
            mod=sorted(module)
            for i in range(len(mod)):
                for j in mod[i+1:]:
                    try:
                        Aij=edgedict[(mod[i],j)]
                    except:
                        Aij=0
                    sumAij+=Aij
                    sumKiKj+=float(Kij[(mod[i],j)])/(2*m)
            Q += float(sumAij)-float(sumKiKj)

        Q=Q/(2*m)
        if Q > MaxM:
            MaxM = Q
            #print MaxM
            Answer = components
            #print(len(Answer))  

t1 = time.time()

total = t1-t0
print total




finans=map(lambda x:list(x),Answer)
finans.sort(key=lambda x: x[0])
with open('Tanay_Shankar_Community.txt', 'wb') as f: 
    for i in finans:
        f.write(str(i))
        f.write('\n')
f.close()

