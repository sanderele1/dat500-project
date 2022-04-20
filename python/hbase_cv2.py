import datasketch as ds
import happybase as hb
import struct
import pickle
import base64
import time
import Hbase_thrift
import random

def hbase_safe_b64_encode(data):
    return base64.b64encode(data).decode('utf8').replace('=', '')

class HBaseDictListStorage(ds.storage.OrderedStorage):
    def __init__(self, config, name):
        self._name = name
        self._table = hbase_safe_b64_encode(name)
        self._pool = config['hbase_pool']
        print(f"Initializing table with name: {self._name}, safe: {self._table}")
        
        needs_create_table = True
        
        retries = 0
        max_retries = 10
        
        while needs_create_table and retries < max_retries:
            try:
                with self._pool.connection() as c:
                    needs_create_table = not c.is_table_enabled(self._table)
            except Hbase_thrift.IOError as e:
                message = e.message.decode('utf8')
                if not message.startswith('org.apache.hadoop.hbase.TableNotFoundException'):
                    raise e
                        
            if needs_create_table:
                sleep_time = random.uniform(0.01, 2)
                print(f"Need to create table:  {self._name} ({self._table}), sleeping {sleep_time} seconds")
                time.sleep(sleep_time)
                print(f"Finished sleeping, attempting to create table ({retries}/{max_retries} retries): {self._name}")
                
                try:
                    with self._pool.connection() as c:
                        families = {
                            'fvalue': dict(),
                        }
                        c.create_table(self._table, families)
                        needs_create_table = False
                        print(f"Successfully create table: {self._name}")
                except BaseException as e:
                    retries += 1
                    print(f"Failed to create table: {self._name}")
                    #raise e
                
        if needs_create_table:
            raise ValueError(f"Failed to create table {self._name} with {retries} retries")
        

    def keys(self):
        raise ValueError('Not implemented')

    def get(self, key):
        raise ValueError('Not implemented')

    def remove(self, *keys):
        raise ValueError('Not implemented')

    def remove_val(self, key, val):
        raise ValueError('Not implemented')

    def insert(self, key, *vals, **kwargs):
        # Needs implementation
        pass
        

    def size(self):
        raise ValueError('Not implemented')

    def itemcounts(self, **kwargs):
        raise ValueError('Not implemented')
        
    def has_key(self, key):
        # Needs implementation
        pass
    
    
class HBaseDictSetStorage(ds.storage.UnorderedStorage, HBaseDictListStorage):
    '''This is a wrapper class around ``defaultdict(set)`` enabling
    it to support an API consistent with `Storage`
    '''
    def __init__(self, config, name=None):
        HBaseDictListStorage.__init__(self, config, name=name)

    def get(self, key):
        pass

    def insert(self, key, *vals, **kwargs):
        HBaseDictListStorage.insert(self, key, *vals, **kwargs)
        
def hbase_ordered_storage(config, name=None):
    #print(f"Overriden ordered storage ran with config: {config}")
    tp = config['type']
    if tp == 'hbase':
        return HBaseDictListStorage(config, name=name)
    else:
        return ds.storage.ordered_storage(config, name=name)


def hbase_unordered_storage(config, name=None):
    #print(f"Overriden unordered storage ran with config: {config}")
    tp = config['type']
    if tp == 'hbase':
        return HBaseDictSetStorage(config, name=name)
    else:
        return ds.storage.unordered_storage(config, name=name)
    
    
def override_lsh__init__(self, threshold=0.9, num_perm=128, weights=(0.5, 0.5),
                 params=None, storage_config=None, prepickle=None, hashfunc=None):
        storage_config = {'type': 'dict'} if not storage_config else storage_config
        self._buffer_size = 50000
        if threshold > 1.0 or threshold < 0.0:
            raise ValueError("threshold must be in [0.0, 1.0]")
        if num_perm < 2:
            raise ValueError("Too few permutation functions")
        if any(w < 0.0 or w > 1.0 for w in weights):
            raise ValueError("Weight must be in [0.0, 1.0]")
        if sum(weights) != 1.0:
            raise ValueError("Weights must sum to 1.0")
        self.h = num_perm
        if params is not None:
            self.b, self.r = params
            if self.b * self.r > num_perm:
                raise ValueError("The product of b and r in params is "
                        "{} * {} = {} -- it must be less than num_perm {}. "
                        "Did you forget to specify num_perm?".format(
                            self.b, self.r, self.b*self.r, num_perm))
        else:
            false_positive_weight, false_negative_weight = weights
            self.b, self.r = ds.lsh._optimal_param(threshold, num_perm,
                    false_positive_weight, false_negative_weight)

        self.prepickle = storage_config['type'] == 'redis' if prepickle is None else prepickle

        self.hashfunc = hashfunc
        if hashfunc:
            self._H = self._hashed_byteswap
        else:
            self._H = self._byteswap

        basename = storage_config.get('basename', ds.storage._random_name(11))
        self.hashtables = [
            hbase_unordered_storage(storage_config, name=b''.join([basename, b'_bucket_', struct.pack('>H', i)]))
            for i in range(self.b)]
        self.hashranges = [(i*self.r, (i+1)*self.r) for i in range(self.b)]
        self.keys = hbase_ordered_storage(storage_config, name=b''.join([basename, b'_keys']))
        
ds.lsh.MinHashLSH.__init__ = override_lsh__init__