import random
import numpy as np

def generateGenomeAndReads(lengde, k, numreads=100, desiredseed=None):
	"""
	generateGenome generates a genome of length "lengde" and
	also returns reads of length k from the genome.

	param lengde: desired genome length.
	param k: desired read lengths, with a default value of k=10, assuming
	lengde > 10. If lengde < 10, a smaller value is used.
	param numreads: desired number of reads with a default value of 100.
	param desiredseed: seed to use for reproduciblity, default None.
	return: returns a string consisting of the randomized assembly amd
	a list consisting of all the reads.
	"""
	if desiredseed:
		random.seed(desiredseed)
	genome = "".join(random.choice("ACGT") for _ in range(lengde))
	reads = []
	for _ in range(numreads):
		# startsequence for read
		startseq = random.randrange(0, len(genome)-k+1)
		reads.append(genome[startseq:startseq+k])
	return genome, reads


def getReadsAndQualities(filename):
	"""
	getReadsAndQualities takes in a FASTQ-file and returns all the
	sequence reads and the corresponding qualities (each in their own lists).

	param filename: name of the file, assumes that the file is FASTQ-formatted.
	return: returns two lists, reads and qualities, each consisting of all the
	sequences (reads) and qualities (qualities) found in the FASTQ file.
	"""
	reads = []
	qualities = []
	with open(filename, 'r') as fil:
		count = 0
		for line in fil:
			if count % 4 == 1:
				reads.append(line.rstrip('\n'))
			elif count % 4 == 3:
				qualities.append(line.rstrip('\n'))
			count += 1
	return reads, qualities

def generateFASTQ(filename):
	"""
	generateFASTQ generates random reads and quality data and writes them to
	"filename. This is used to get quick, fast, and easy access to some FASTQ
	data for testing purposes.

	param filename: name of file to write FASTQ data too.
	"""
	DNA = "ACGT"
	readlength = 151
	with open(filename, 'w') as fil:
		for j in range(0,1000):
			read = ""
			quality = ""
			fil.write("@TULLEGREIER READ NUMMER BLA BLA SEQ DITT DATT\n")
			for i in range(readlength):
				read += DNA[random.randint(0,3)]
			fil.write(read + "\n")
			fil.write("+TULLEGREIER READ NUMMER BLA BLA SEQ DITT DATT\n")
			for i in range(readlength):
				quality += "1234567890"[random.randint(0,len("1234567890")-1)]
			fil.write(quality + "\n")
	fil.close()

def printGCstatistics(genome):
	"""
	Prints information about the GC content in genome
	For reference, humans should have a GC-content of around 41% (on average
	and given long enough reads. see https://bmcresnotes.biomedcentral.com/articles/10.1186/s13104-019-4137-z)
	"""
	GCcounter = 0
	for base in genome:
		if base == "C" or base == "G":
			GCcounter += 1

	print("Total GC-reads: {}".format(GCcounter))
	print("Total bases in the genome: {}".format(len(genome)))
	print("The GC content in the genome is: {}".format(GCcounter/len(genome)))

if __name__ == "__main__":
	
	#######
	# test GCstatistics
	# genome = ""
	# for i in range(0,1000000):
	# 	genome += "ACGT"[random.randint(0,3)]
	# printGCstatistics(genome)


	#######
	# generate some "fastQ"-formated data for testing
	# it's actually just a .txt file but formated like
	# a fastQ-file :-)
	# generateFastQ("test.txt")
	# reads, qualities = getReadsAndQualities("test.txt")
	# print(reads[:3])
	# print(qualities[:3])

	# genome, reads = generateGenomeAndReads(100000, 15, 1000)
	# print(reads)
	# print(genome)