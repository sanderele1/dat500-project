### check to see the range of qualities
from black import diff


def qualityExtractor(filnavn="SRR17981961.head"):
    """
    Returns all the qualities as one string
    """
    with open(filnavn, 'r') as fil:
        qualities = ""
        count = 0
        for line in fil:
            if count % 4 == 3:
                qualities += line.rstrip('\n')
            count += 1
        return qualities, filnavn

if __name__ == "__main__":
    quals, filnavn = qualityExtractor()
    differentqualities = set()
    for q in quals:
        differentqualities.add(q)
    print("These are all the different qualities in the file {}:".format(filnavn))
    print(differentqualities)
    print("We have {} different scores.".format(len(differentqualities)))

    ordlist =  []
    for dif in differentqualities:
        ordlist.append(ord(dif))
    print("Here are the ASCII values for the same qualities:")
    print(ordlist)
    print("The lowest quality score found is {} in ASCII.".format(min(ordlist)))
    print("The lowest quality score found is {} in ASCII.".format(max(ordlist)))