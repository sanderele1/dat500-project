from mrjob.job import MRJob
from mrjob.step import MRStep
import mrjob.protocol

import base64
import io


class MRStepFastqToJson(MRStep):
    def __init__(self, *args, **kwargs):
        MRStep.__init__(self, *args, mapper=self.mapper, **kwargs)

        self.hasLocated = False
        self.lines = []

    def mapper(self, _, line):
        # Empty line, return
        if len(line.strip()) < 1:
            return

        if not self.hasLocated:

            # We're misaligned, we're at a sequence read.
            if len(line) > 50:
                return

            # We're either at a a gene definition, or at a quality definition

            # We're at quality, not gene. Misaligned
            if line[0] != '@':
                return

            # We're not at a gene definition, aligned.
            self.hasLocated = True

        self.lines.append(line)
        if len(self.lines) == 4:
            yield "lines", {
                'gene-def': self.lines[0],
                'gene': self.lines[1],
                'quality-def': self.lines[2],
                'quality': self.lines[3]
            }
            self.lines = []



class MRStepJsonToG1(MRStep):
    def __init__(self, *args, **kwargs):
        MRStep.__init__(self, *args, mapper=self.mapper, **kwargs)

    def map_basepair(basepair):
        if basepair == "P":
            return 0
        if basepair == "A":
            return 1
        elif basepair == "T":
            return 2
        elif basepair == "C":
            return 3
        elif basepair == "G":
            return 4
        elif basepair == "N":
            return 5
        else:
            raise ValueError("Basepair was {}, which is not a subset of \"ATCG\"".format(basepair))

    def inv_map_basepair(basepair):
        if basepair == 0:
            return "P"
        elif basepair == 1:
            return "A"
        elif basepair == 2:
            return "T"
        elif basepair == 3:
            return "C"
        elif basepair == 4:
            return "G"
        elif basepair == 5:
            return "N"
        else:
            raise ValueError("Basepair was {}, which is not either \"0\", \"1\", \"2\", \"3\", \"4\" or \"5\"".format(basepair))

    def mapper(self, _, value):
        gene_def = value['gene-def']
        
        gene_def_identifier = gene_def[1:gene_def.find(" ")]

        gene = value['gene']

        gene_buffer = io.BytesIO()
        for i in range(0, len(gene), 2):
            g0 = gene[i] 
            g1 = gene[i+1] if i < len(gene) - 1 else "P"
            byte = (MRStepJsonToG1.map_basepair(g0)) | (MRStepJsonToG1.map_basepair(g1) << 4)
            gene_buffer.write(bytes([byte]))

        gene_base64 = base64.b64encode(gene_buffer.getbuffer())

        quality = value['quality']

        quality_buffer = io.BytesIO()
        for q in quality:
            quality_buffer.write(bytes([ord(q) - 0x21]))

        quality_base64 = base64.b64encode(quality_buffer.getbuffer())

        return "G1", {'i': gene_def_identifier, 'g': gene_base64, 'q': quality_base64}
        # !"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\]^_`abcdefghijklmnopqrstuvwxyz{|}~


class MRJobFastqToIntermediate(MRJob):
    OUTPUT_PROTOCOL = mrjob.protocol.JSONProtocol

    def steps(self):
        return [MRStepFastqToJson(), MRStepJsonToG1()]

if __name__ == '__main__':
    MRJobFastqToIntermediate.run()
