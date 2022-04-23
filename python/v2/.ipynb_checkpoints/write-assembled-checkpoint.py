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



import mrjob.protocol

def run_buffer(buffer):
    pass

class CanMrJobDoThis(MRJob):
    
    INPUT_PROTOCOL = mrjob.protocol.JSONValueProtocol
    OUTPUT_PROTOCOL = mrjob.protocol.JSONValueProtocol
    
    def configure_args(self):
        super(CanMrJobDoThis, self).configure_args()
        self.add_passthru_arg('--table', type=str, help="The hbase table prefix to insert data into")

    
#    def configure_args(self):
#        super(CanMrJobDoThis, self).configure_args()
#        self.add_passthru_arg('--buffersize', type=int, default=50, help="How many values to buffer before inserting")

    
    def mapper_init(self):
        # Disable logging: otherwise may cause issues with TSocket from the hbase connection library. Although probably not neccesary in raw mrjob, as stderr should be a stringstream, disable it just incase. Errors will be raised regardless.
        
        logging.disable(logging.CRITICAL)
        self._pool = hbase_connector.HbaseConnection(host="localhost", port=9090)
        #self._table = b'assembledTable'
        self._table = self.options.table.encode('utf8')
        self.buffer = []
        
        try:
            with self._pool as c:
                if not c.tableExists(TTableName(qualifier=self._table)):
                    key_family = TColumnFamilyDescriptor(name=b'key', bloomnFilterType=TBloomFilterType.ROWCOL)
                    value_family = TColumnFamilyDescriptor(name=b'value')
                    name = TTableName(qualifier=self._table)
                    newTable = TTableDescriptor(tableName=name, columns=[key_family, value_family])
                    c.createTable(newTable, None)
        except TIOError as e:
            message = e.message.decode('utf8')
            if not message.startswith('org.apache.hadoop.hbase.TableNotFoundException'):
                raise e
        
        
    def createPut(self, value):
        puts = []
        raw_read = value[0]
        raw_matches = value[1]
        
        read_index = raw_read[0]
        read_value = raw_read[1]
        for raw_match in raw_matches:
            match_index = raw_match[0][0]
            match_value = raw_match[0][1]
            
            # match_comparison is (ed, score, cigar, cigarv2, ...)
            # From our mrjob_ass.py, all candidates we get have the same edit score
            match_comparison = raw_match[1]
            
            # What we want to do, is to create a table in the database.
            # We then want to insert for each base in our read, compute the position of that base from the candidate.
            # Then put a row with the key = "position_of_base-base". So if the base is supposed to be A, we write f.ex: "214_A".
            # For the column, we give it the index of the read. This ensures that our insertion is idempotent.
            
            # We could have used atomicincrement instead, which would probably have been much faster. But that will fail if something crashes. As we have no idea how far it got.
            for i, base in enumerate(read_value):
                base_position = match_index + i
                cols = [TColumnValue(family=b'key', qualifier=read_index.to_bytes(8), value=b'') for v in vals]
                put = TPut(row=f"{base_position}-{base}".encode('utf8'), columnValues=cols, durability=TDurability.SKIP_WAL)
                puts.append(put)
            #c.putMultiple(self._table, puts)
        return puts
    
    def runInternal(self):
        puts = []
        for v in self.buffer:
            puts.extend(self.createPut(v))
        with self._pool as c:
            c.putMultiple(self._table, puts)
        self.buffer.clear()
    
    def mapper(self, _, value):
        self.buffer.append(value)
        if len(self.buffer) > 10:
            self.runInternal()
            
    def mapper_final(self):
        if len(self.buffer) > 0:
            self.runInternal()
        
if __name__ == '__main__':
    #mp.set_start_method('spawn')
    CanMrJobDoThis.run()
    
