import graphviz

def DeBruijnFromString(streng, kmer):
    edges = []
    nodes = []
    # loop through every kmer in streng
    for i in range(0, len(streng) - kmer + 1):
        # left and right k-1mers
        leftmer = streng[i:i+kmer-1]
        rightmer = streng[i+1:i+kmer]
        edges.append((leftmer, rightmer))
        # no repeats
        if leftmer not in nodes:
            nodes.append(leftmer)
        if rightmer not in nodes:
            nodes.append(rightmer)
    return nodes, edges

# nodes, edges = DeBruijnFromString("AABABABBABABABAAAB", 3)
# print(nodes)
# print(edges)
# the order of edges -> eulerian walk!

def DeBruijnGrapher(streng, kmer):
    # Written by Ben Langmead and Jacob Pritt, copied from them
    # (not verbatim)
    nodes, edges = DeBruijnFromString(streng, kmer)
    dot_str = 'digraph "DeBruijn graph" {\n'
    for node in nodes:
        dot_str += '    {} [label="{}"] ;\n'.format(node, node) 
    for source, destination in edges:
        dot_str += '    {} -> {} ;\n'.format(source, destination)
    return dot_str + '}\n'

# print(DeBruijnGrapher("AABABABBABABABAAAB", 3))
d = DeBruijnGrapher("AABABABBABABABAAAB", 3)
print(d)
dot = graphviz.Digraph(d, filename="debruijn.gv")
dot.view()
# print(dot)
# doctest_mark_exe()
# dot.render('debruijn.gv').replace('\\', '/')