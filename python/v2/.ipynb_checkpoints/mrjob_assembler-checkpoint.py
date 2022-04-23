import sys
sys.path.append('/home/ubuntu/.local/lib/python3.8/site-packages')
sys.path.append('/home/ubuntu/GenASM/build/lib.linux-x86_64-3.8') # For the homebrew GenASM bindings in C. These need to be added to the datanodes too.
sys.path.append('./')

import json
import pickle
import base64

import gasm # Homebrew package
import datasketch as ds
import hbase_connector
from multiprocessing import Process, Queue, Pipe
import time

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

def f(data):    
    pass
        #pool = hbase_connector.HbaseConnection(host="localhost", port=9090)
        #lsh = ds.lsh.MinHashLSH(threshold=0.8, num_perm=128, storage_config={'type': 'hbase', 'basename': b'hbase_salmonella_pos_prefix_8', 'hbase_pool': pool}, prepickle=True)
        #matches = []
        
        
        
#    except BaseException as e:
#        q.put(str(e))
#    finally:
#        q.close()
#        q.join_thread()
    #    try: 
        #sys.stderr.write(f"F: Entry with {len(data)}\n")
#        pool = hbase_connector.HbaseConnection(host="localhost", port=9090)

        # Table is from run_insert.sh
#        lsh = ds.lsh.MinHashLSH(threshold=0.8, num_perm=128, storage_config={'type': 'hbase', 'basename': b'hbase_salmonella_pos_prefix_8', 'hbase_pool': pool}, prepickle=True)

#    except BaseException as e:
#        queue.put(e)

        
#        matches = []

        #sys.stderr.write(f"F: Decoding data\n")

#        data_b = [base64.b64decode(d) for d in data]
#        data_p = [pickle.loads(b) for b in data_b]


        #sys.stderr.write(f"F: Beggining query\n")
#        for read_number, read in data_p:
#            candidate_matches = lsh.query(create_hash(read))
#            qualities = [(candidate, gasm.gasmAlignment(read, candidate[1], 20, 20, 4, 5, 1)) for candidate in candidate_matches]
#            qualities = list(filter(lambda x: x[1][2] != '', qualities))
#            qualities.sort(key=lambda x: x[1][0])
#            if len(qualities) > 0:
#                match = qualities[0]
#                matches.append(((read_number, read), match))

        #sys.stderr.write(f"F: Put\n")
#        queue.put(matches)
        #sys.stderr.write(f"F: Close\n")
#        queue.close()
        #sys.stderr.write(f"F: Join thread\n")
#        queue.join_thread()
        #sys.stderr.write(f"F: Exiting\n")
    

buffer_size = 100
processes = 2
    
def run_buffer(buffer):
    #print("hello")
    
    #sys.stderr.write(f"Running buffer: {len(buffer)}\n")
    #q = Queue()
    procs = []
    chunky = list(chunks(buffer, len(buffer) // processes))
    for ch in chunky:
        #sys.stderr.write(f"Spinning up process: {data}\n")
        #pip, child = Pipe()
        p = Process(target=f, args=(ch,), daemon=True)
        procs.append(p)
        
    
    
    #sys.stderr.write(f"Going to start: {len(procs)}\n")
    
    for proc in procs:
        #sys.stderr.write(f"Starting process\n")
        proc.start()
        #sys.stderr.write(f"Successfully started process \n")
    
    #sys.stderr.write(f"Has started all of the processes\n")
    
    results = [] 
    #for proc in procs:
    #    sys.stderr.write("Waiting for response\n")
    #    r = q.get()
    #    sys.stderr.write(f"Got result: {r}\n")
    #    results.append(r)
    
    for proc in procs:
        proc.join()
        #sys.stderr.write(f"Joined process\n")
        
    #sys.stderr.write(f"Returning\n")
    
    #return results

class CanMrJobDoThis(MRJob):
    def mapper_init(self):
        self.buffer = []
        
    def mapper(self, _, value):
        self.buffer.append(value)
        if len(self.buffer) >= buffer_size:
            run_buffer(self.buffer)
            self.buffer.clear()
            
    def mapper_final(self):
        return
        if len(self.buffer) > 0:
            run_buffer(self.buffer)
            self.buffer.clear()

if __name__ == '__main__':
    CanMrJobDoThis.run()
    
# -Dmapreduce.input.fileinputformat.split.maxsize=1    

#python3 mrjob_assembler.py --hadoop-streaming-jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.1.jar -r hadoop hdfs:///files/testdata.hadoop --files hbase_connector.py,gasm.cpython-38-x86_64-linux-gnu.so


# python3 mrjob_assembler.py -r inline testdata.hadoop --files hbase_connector.py,gasm.cpython-38-x86_64-linux-gnu.so

