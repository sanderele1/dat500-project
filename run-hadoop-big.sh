#hadoop fs -rm -r /files/SRR17981961_1.G1
#hadoop fs -rmdir /files/SRR17981961_1.G1

python3 conversion.py --hadoop-streaming-jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.1.jar -r hadoop hdfs:///files/SRR17981961_1.fastq --output-dir hdfs:///files/SRR17981961_1.G1 --no-cat-output

## Logs can be found on the datanodes: /usr/local/hadoop/logs/userlogs/application_*/container_*
## Example: /usr/local/hadoop/logs/userlogs/application_1645801677219_0003/container_1645801677219_0003_01_000006/stderr
 
