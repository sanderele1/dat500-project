import sys
sys.path.append('/home/ubuntu/.local/lib/python3.8/site-packages')
sys.path.append('/home/ubuntu/GenASM/build/lib.linux-x86_64-3.8') # For the homebrew GenASM bindings in C. These need to be added to the datanodes too.
sys.path.append('./')

import json
import pickle
import base64

import gasm # Homebrew package
import datasketch as ds

# Import logging is critical, as TSocket tries to log stuff when there is an error, but this fails in mrjob multiprocess as a binary stderr is assumed.
import logging
import hbase_connector

import multiprocessing as mp
from multiprocessing import Process, Queue, Pipe, Pool
import time
import gc

# These are the logging namespaces
# ["redis.cluster", "redis", "concurrent.futures", "concurrent", "asyncio", "thrift.transport.TSocket", "thrift.transport", "thrift", "mrjob.util", "mrjob", "mrjob.conf", "mrjob.parse", "mrjob.options", "mrjob.compat", "mrjob.fs.base", "mrjob.fs", "mrjob.fs.composite", "mrjob.fs.local", "mrjob.setup", "mrjob.step", "mrjob.runner", "mrjob.job", "__main__", "mrjob.sim", "mrjob.inline"]







from mrjob.job import MRJob

    
def create_hash(string, nperm=128):
    mh2 = ds.MinHash(num_perm=nperm)
    for i, c in enumerate(string):
        mh2.update(i.to_bytes(8, byteorder='little') + c.encode('utf8'))
    return mh2
    
# https://stackoverflow.com/questions/312443/how-do-you-split-a-list-into-evenly-sized-chunks
def chunks(l, n):
    n = max(1, n)
    return (l[i:i+n] for i in range(0, len(l), n))    

def run_chunk(chunk, q):
    #bad_loggers = ["thrift.transport.TSocket", "thrift.transport", "thrift"]

    #for bad_logger in bad_loggers:
    #    log = logging.getLogger(bad_logger)
    #    log.propagate = False

    logging.disable(logging.CRITICAL)
    
    pool = hbase_connector.HbaseConnection(host="localhost", port=9090)
    lsh = ds.lsh.MinHashLSH(threshold=0.8, num_perm=128, storage_config={'type': 'hbase', 'basename': b'hbase_salmonella_pos_prefix_8', 'hbase_pool': pool}, prepickle=True)
    
    data_b = [base64.b64decode(d) for d in chunk]
    data_p = [pickle.loads(b) for b in data_b]
        
        
    matches = []
    
    for read_number, read in data_p:
        candidate_matches = lsh.query(create_hash(read))
        qualities = [(candidate, gasm.gasmAlignment(read, candidate[1], 20, 20, 4, 5, 1)) for candidate in candidate_matches]
        qualities = list(filter(lambda x: x[1][2] != '', qualities))
        #qualities.sort(key=lambda x: x[1][0])
        if len(qualities) > 0:
            #match = qualities[0]
            
            ed = min(map(lambda x: x[1][0], qualities))
            filtered = filter(lambda x: x[1][0] == ed, qualities)
            
            matches.append(((read_number, read), list(filtered)))
            #matches.append(qualities)
            
     

    
    #min_ed = min(matches, key=lambda x: x[1][1][0])
    #matches = filter(lambda x: x[1][1][0] == min_ed, matches)
    
    #m = min(map(lambda x: x[1], h))
    #list(filter(lambda x: x[1] == m, h))
    
    #pool._transport.close()
    #pool._socket.close()
    
    for e in matches:
        q.put(e)
    
    #q.put((data_p,))
    q.close()
    
    
    #loggers = [logging.getLogger(name) for name in logging.root.manager.loggerDict]
    return list(logging.root.manager.loggerDict)

def run_buffer(buffer):
    chunkified = chunks(buffer, len(buffer) // 3)
    
    ps = []
    q = Queue()
    
    for chunk in chunkified:
        p = Process(target=run_chunk, args=(chunk,q,), daemon=True)
        ps.append(p)
        
    for p in ps:
        p.start()
        
    for p in ps:
        p.join()
        
    values = []
    while not q.empty():
        values.append(q.get())
        
    return values
    
#    with Pool(5) as p:
#        v = p.map(run_chunk, buffer)
#        #raise ValueError('returned')
#    return v

import mrjob.protocol

class CanMrJobDoThis(MRJob):
    
    OUTPUT_PROTOCOL = mrjob.protocol.JSONValueProtocol
    
    def mapper_init(self):
        self.buffer = []
        
    def mapper(self, _, value):
        #self.max -= 1
        #if self.max <= 0:
        #    #yield "Skipping", value
        #    return
        
        self.buffer.append(value)
        if len(self.buffer) >= 50:
            #v =
            #raise ValueError('returned buffer')
            for v in run_buffer(self.buffer):
                yield None, v
            #yield "Running once", ""
            self.buffer.clear()
            gc.collect()
            
    def mapper_final(self):
        if len(self.buffer) > 0:
            for v in run_buffer(self.buffer):
                yield None, v
            self.buffer.clear()
            gc.collect()
            #yield "Running once", ""

if __name__ == '__main__':
    #mp.set_start_method('spawn')
    CanMrJobDoThis.run()
    
# python3 mrjob_ass.py -r inline testdata.hadoop --files hbase_connector.py,gasm.cpython-38-x86_64-linux-gnu.so
# python3 mrjob_ass.py --hadoop-streaming-jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.1.jar -r hadoop hdfs:///files/testdata.hadoop --files hbase_connector.py,gasm.cpython-38-x86_64-linux-gnu.so

# BIG BOI: python3 mrjob_ass.py --hadoop-streaming-jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.1.jar -r hadoop hdfs:///files/salmonella/SRR15404285.pickleb64 --files hbase_connector.py,gasm.cpython-38-x86_64-linux-gnu.so --output-dir hdfs:///files/salmonella/matches_v4 -Dmapreduce.task.timeout=7200000

