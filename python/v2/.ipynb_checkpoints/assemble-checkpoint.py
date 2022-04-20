#!/usr/bin/env python
# coding: utf-8

# import findspark
# findspark.init()
# findspark.find()

# from pyspark.sql import SparkSession
# 
# spark = (SparkSession
#          .builder
#          .master("yarn")
#          .appName("python-testing")
#          .config("spark.executor.instances", 1)
#          .config("spark.executor.memory", "1g")
#          .getOrCreate())
# sc = spark.sparkContext
# sc

# reads = sc.textFile('hdfs:///files/salmonella/SRR15404285.fasta').filter(lambda x: not x.startswith('>'))
# reads.take(1)

# reads.count()

# test_reads = reads.take(5)
# test_reads

# In[1]:


test_reads = ['TGCCGNCCTGAGCGAAAGCCTGCTGGAAGAAGTAGCTTCGCTGGTGGAATGGCCGGTGGTATTGACGGCGAAATT',
 'GGGTTNATCCAGACTTCATCCGGCACCGCCTCATGCAGCATCAGCACATTGCTGTAGGTCGAGTGGGTATGCCCT',
 'CCCAAAGATACGGGCGCAGAAAAGGCCGTCACGCTCAGGTTTGAACGTACGGTAGTTGATGGTTTCCGGCTTTTT',
 'CTCACGGAGAAAAGCGAAAATAAACGATTGACTCTGAAGCGGGAAAGCGTAATATGCACACCACGCGACGCTGAG',
 'TGACCGTTTACGCGCCTGCCGTACCGCGCAGGAAGTCCTGGATCTCATTGACCGCACCAACGCGGCAGCTTAAGA']


# In[2]:


import sys
sys.path.append('/home/ubuntu/.local/lib/python3.8/site-packages')
sys.path.append('/home/ubuntu/GenASM/build/lib.linux-x86_64-3.8') # For the homebrew GenASM bindings in C. These need to be added to the datanodes too.

import json
import pickle
import base64

import gasm # Homebrew package
import datasketch as ds
import hbase_connector


# In[3]:


pool = hbase_connector.HbaseConnection(host="datanode1", port=9090)
# Table is from run_insert.sh
lsh = ds.lsh.MinHashLSH(storage_config={'type': 'hbase', 'basename': b'hbase_salmonella_windowed', 'hbase_pool': pool}, prepickle=True)


# In[4]:


def create_hash(string, nlet=3):
    mh2 = ds.MinHash()
    for i in range(nlet, len(string)):
        v = string[i-nlet:i]
        mh2.update(v.encode('utf8'))
    return mh2


# In[5]:


get_ipython().run_cell_magic('time', '', 'for read in test_reads:\n    h = create_hash(read, 3)\n    lsh.query(h)\n    #print(lsh.query(h))')


# ## GenASM docs:
# ```python
# genasm_aligner(<reference sequence>,
#                <query sequence>,
#                <edit distance threshold>,
#                <match score>,
#                <substitution penalty>,
#                <gap-opening penalty>,
#                <gap-extension penalty>)
# ```

# In[6]:


gasm.__dict__


# In[7]:


# genasm_aligner(<reference sequence>, <query sequence>, <edit distance threshold>, <match score>, <substitution penalty>, <gap-opening penalty>, <gap-extension penalty>)
gasm.gasmAlignment("AATGTCC", "ATATGTCC", 3, 3, 4, 5, 1)


# In[ ]:


get_ipython().run_cell_magic('time', '', 'for read in test_reads:\n    h = create_hash(read, 3)\n    candidates = lsh.query(h)\n    print(candidates)\n    if len(candidates) > 0:\n        scores = []\n        for i, cand in enumerate(candidates):\n            print(f"Attempting {i}: {cand}")\n            print(gasm.gasmAlignment(read, cand[1], 20, 18, 4, 5, 1))\n            #scores = [(cand[0], cand[1], gasm.gasmAlignment(read, cand[1], 20, 18, 4, 5, 1)) for cand in candidates]\n        #scores = [(cand[0], cand[1], cand[1]) for cand in candidates]\n        #scores.sort(key=lambda x: x[2][0])\n        #print(scores)')


# In[8]:


[2, 3, 0, 1].sort(key=lambda x: x)


# In[9]:


[2, 3, 0, 1].sort


# In[10]:


read = test_reads[4][3]
h = create_hash(read, 3)
candidates = lsh.query(h)
candidates


# In[11]:


gasm.gasmfunc(read, candidates[0][1], 20, 18, 4, 5, 1)


# In[ ]:




