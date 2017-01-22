# how many line we can hold in memory
cacheSize = 50

# sort lines as Integer value
asInt = False


def sortKey(x):
    return int(x) if asInt else x


def comparator(x1, x2):
    return sortKey(x1) < sortKey(x2)
