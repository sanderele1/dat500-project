from mrjob.job import MRJob


class MRJobFastqToIntermediate(MRJob):

    def __init__(self):
        pass
        #self.HADOOP_INPUT_FORMAT = 'org.apache.hadoop.mapred.lib.NLineInputFormat'
        #self.JOBCONF = {'mapred.line.input.format.linespermap': 4}

    def mapper(self, key, line):
        pass

    def reducer(self, key, values):
        pass#yield key, sum(values)


if __name__ == '__main__':
    MRJobFastqToIntermediate.run()