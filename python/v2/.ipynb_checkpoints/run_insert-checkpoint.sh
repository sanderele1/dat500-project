# Table with quadlets / create_hash(..., 3): hbase_salmonella_windowed
# Table with pos-prefix / mh2.update(i.to_bytes(8, byteorder='little') + c.encode('utf8')): hbase_salmonella_pos_prefix
# Table with pos-prefix / mh2.update(i.to_bytes(8, byteorder='little') + c.encode('utf8')) + 0.5 threshold: hbase_salmonella_pos_prefix_2

# Serious attempt with threshold=0.7, num_perm=256 - hbase_salmonella_pos_prefix_7 and insert:
#        def create_hash(string, num_perm=256):
#            mh2 = ds.MinHash(num_perm=num_perm)
#            for i, c in enumerate(string):
#                mh2.update(i.to_bytes(8, byteorder='little') + c.encode('utf8'))
#            return mh2


# self.lsh = ds.lsh.MinHashLSH(threshold=0.8, num_perm=128, storage_config={'type': 'hbase', 'basename': table, 'hbase_pool': self.pool}, - hbase_salmonella_pos_prefix_8

python3 hbase_insert.py --table hbase_salmonella_pos_prefix_8 --hadoop-streaming-jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.1.jar -r hadoop hdfs:///files/salmonella/window.b64pickled --output-dir hdfs:///files/salmonella/hbase_salmonella_pos_prefix_8 --files hbase_connector.py
