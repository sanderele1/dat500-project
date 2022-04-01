import datasketch as ds
import happybase as hb
import struct
import pickle
import base64
import time

class HBaseDictListStorage(ds.storage.OrderedStorage):
    def __init__(self, config, name=None):
        self._name = b'' if name == None else name
        self._pool = config['hbase_pool']
        #self._table = config['hbase_table']
        self._table = base64.b64encode(config['hbase_table'].encode('utf8') + self._name).decode('utf8').replace('=', '')
        self._keysize = config['hbase_keysize']
        self._recreate_table = config.get('hbase_recreate_table', False)
        
        with self._pool.connection() as c:
            families = {
                'fvalue': dict(),
            }
            
            tables = c.tables()
            if not self._table.encode('utf8') in tables:
                #print(f"Trying to create table with name: {self._table}, and type: {type(self._table)}")
                try:
                    c.create_table(self._table, families)
                except Exception as ex:
                    time.sleep(3)
                    pass #print(f"Exception attempting to create table: {ex}")
                #raise ValueError(f"Table \"{self._table}\" must already exist. You need to create it.")
            elif self._recreate_table:
                try:
                    #print(f"Deleting table with name: {self._table}")
                    c.delete_table(self._table, disable=True)
                    #print(f"Trying to create table with name: {self._table}, and type: {type(self._table)}")
                    c.create_table(self._table, families)
                except Exception as ex:
                    time.sleep(3)
                    pass #print(f"Exception attempting to delete and re-create able: {ex}")
                

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
        
        
        
        #if len(key) != self._keysize:
        #    raise ValueError(f'Length of key must be equal to config parameter: "hbase_keysize", was {len(key)}, should be: {self._keysize}')
        
        binary_key = key.encode('utf8') if type(key) is str else key
        
        #print(f"Insert: key: {binary_key}, kt: {type(binary_key)}, vals: {vals}, vt: {type(vals[0])}")
        
        with self._pool.connection() as c:
            table = c.table(self._table)
            with table.batch() as batch:
                for value in vals:
                    value = value.encode('utf8') if type(value) is str else value
                    #print(f"Trying to insert values with key: {binary_key + value}")
                    batch.put(binary_key + value, {b'fvalue:value': value})


    def size(self):
        raise ValueError('Not implemented')

    def itemcounts(self, **kwargs):
        raise ValueError('Not implemented')
        
    def has_key(self, key):
        # Needs implementation
        with self._pool.connection() as c:
            table = c.table(self._table)
            row = table.row(key)
            #print(f"Checking whether key: {key} exists: {len(row) != 0}, (row: {row})")
            return len(row) != 0
        
        
class HBaseDictSetStorage(ds.storage.UnorderedStorage, HBaseDictListStorage):
    '''This is a wrapper class around ``defaultdict(set)`` enabling
    it to support an API consistent with `Storage`
    '''
    def __init__(self, config, name=None):
        HBaseDictListStorage.__init__(self, config, name=name)

    def get(self, key):
        with self._pool.connection() as c:
            table = c.table(self._table)
            values = [row[1][b'fvalue:value'] for row in table.scan(row_prefix=key)]
            return set(values)

    def insert(self, key, *vals, **kwargs):
        HBaseDictListStorage.insert(self, key, *vals, **kwargs)
        
        
def hbase_ordered_storage(config, name=None):
    #print(f"Overriden ordered storage ran with config: {config}")
    tp = config['type']
    if tp == 'hbase':
        return HBaseDictListStorage(config, name=name)
    else:
        return old_ordered_storage_function(config, name=name)

if ds.storage.ordered_storage != hbase_ordered_storage:
    old_ordered_storage_function = ds.storage.ordered_storage
    ds.storage.ordered_storage = hbase_ordered_storage
    
    
def hbase_unordered_storage(config, name=None):
    #print(f"Overriden unordered storage ran with config: {config}")
    tp = config['type']
    if tp == 'hbase':
        return HBaseDictSetStorage(config, name=name)
    else:
        return old_unordered_storage_function(config, name=name)

if ds.storage.unordered_storage != hbase_unordered_storage:
    old_unordered_storage_function = ds.storage.unordered_storage    
    ds.storage.unordered_storage = hbase_unordered_storage

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
            ds.storage.unordered_storage(storage_config, name=b''.join([basename, b'_bucket_', struct.pack('>H', i)]))
            for i in range(self.b)]
        self.hashranges = [(i*self.r, (i+1)*self.r) for i in range(self.b)]
        self.keys = ds.storage.ordered_storage(storage_config, name=b''.join([basename, b'_keys']))
        
ds.lsh.MinHashLSH.__init__ = override_lsh__init__