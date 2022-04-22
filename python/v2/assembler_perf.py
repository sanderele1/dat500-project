import sys
sys.path.append('/home/ubuntu/.local/lib/python3.8/site-packages')
sys.path.append('/home/ubuntu/GenASM/build/lib.linux-x86_64-3.8') # For the homebrew GenASM bindings in C. These need to be added to the datanodes too.

import json
import pickle
import base64

import gasm # Homebrew package
import datasketch as ds
import hbase_connector
from multiprocessing import Process, Queue
import time

with open('testset.pickle', 'rb') as f:
    dataset = pickle.load(f)[0:10000]

    
def create_hash(string, nperm=128):
    mh2 = ds.MinHash(num_perm=nperm)
    for i, c in enumerate(string):
        mh2.update(i.to_bytes(8, byteorder='little') + c.encode('utf8'))
    return mh2
    
# https://stackoverflow.com/questions/312443/how-do-you-split-a-list-into-evenly-sized-chunks
def chunks(l, n):
    n = max(1, n)
    return (l[i:i+n] for i in range(0, len(l), n))    

def f(queue, data):
    pool = hbase_connector.HbaseConnection(host="datanode1", port=9090)
    # Table is from run_insert.sh
    lsh = ds.lsh.MinHashLSH(threshold=0.8, num_perm=128, storage_config={'type': 'hbase', 'basename': b'hbase_salmonella_pos_prefix_8', 'hbase_pool': pool}, prepickle=True)
    matches = []

    for read_number, read in data:
        candidate_matches = lsh.query(create_hash(read))
        qualities = [(candidate, gasm.gasmAlignment(read, candidate[1], 20, 20, 4, 5, 1)) for candidate in candidate_matches]
        qualities = list(filter(lambda x: x[1][2] != '', qualities))
        qualities.sort(key=lambda x: x[1][0])
        if len(qualities) > 0:
            match = qualities[0]
            matches.append(((read_number, read), match))
            
    print("Proc finished")
    queue.put(matches)
    queue.close()
    queue.join_thread()

    
print(f"Running with {len(dataset)} elements")    

if __name__ == '__main__':
    processes = 20
    q = Queue()
    procs = []
    for data in chunks(dataset, len(dataset) // processes):
        p = Process(target=f, args=(q, data))
        p.daemon = True
        procs.append(p)
    
    for proc in procs:
        proc.start()
    
    results = []
    for proc in procs:
        results.append(q.get())
    
    for proc in procs:
        proc.join()
        
    print("Done")