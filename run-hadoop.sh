hadoop fs -rm -r /dis_materials/output1
hadoop fs -rmdir /dis_materials/output1

python3 conversion.py --hadoop-streaming-jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.1.jar -r hadoop testdata/SRR17981961.head --output-dir hdfs:///dis_materials/output1 --no-cat-output

## Logs can be found on the datanodes: /usr/local/hadoop/logs/userlogs/application_*/container_*
## Example: /usr/local/hadoop/logs/userlogs/application_1645801677219_0003/container_1645801677219_0003_01_000006/stderr
