import os
import heapq

# how many elements we can hold in memory
sizeBlock = 10

# sort lines as Integer value
asInt = True


def sortKey(x):
    return int(x) if asInt else x


def comparator(x1, x2):
    return sortKey(x1) < sortKey(x2)


class Stream:
    _chunkSize = 10

    def __init__(self, path):
        self.file = open(path, 'r')
        self.elements = []
        self._readNextChunk()

    def __bool__(self):
        return bool(self.elements)

    def __lt__(self, other):
        return comparator(self.next(), other.next())

    def next(self):
        assert(len(self.elements) > 0)
        return self.elements[0]

    def pop(self):
        element = self.elements[0]
        self.elements.pop(0)

        if not element:
            self._readNextChunk()

        return element

    def _readNextChunk(self):
        try:
            for i in range(self._chunkSize):
                line = self.file.readline()
                if line:
                    self.elements.append(line.strip('\n'))
        except EOFError as e:
            pass

    def close(self):
        if self.file:
            self.file.close()


class SorterHelper:
    def __init__(self, paths):
        self.streams = []
        for path in paths:
            heapq.heappush(self.streams, Stream(path))

    def __iter__(self):
        while self.streams:
            smallest = heapq.heappop(self.streams)

            yield smallest.pop()

            if smallest:
                heapq.heappush(self.streams, smallest)
            else:
                smallest.close()


class Sorter:
    def sort(self, filename):
        self.paths = []
        self.splitAndSort(filename)

        # for debug
        filename += '_dest'

        with open(filename, 'w') as f:
            for elem in SorterHelper(self.paths):
                f.write(elem + '\n')

        self.deleteTempFiles()

    def splitAndSort(self, filename):
        with open(filename, 'r') as f:
            cache = []
            for line in f:
                if not line:
                    continue

                cache.append(line.strip('\n'))
                # TODO: current line may be very big, change to read byte
                if len(cache) >= sizeBlock:
                    self.dumpToFile(cache)
                    cache.clear()

        if cache:
            self.dumpToFile(cache)

    def createNewFile(self):
        self.paths.append('__tempFileForSort' + str(len(self.paths)))
        return self.paths[-1]

    def deleteTempFiles(self):
        for path in self.paths:
            os.remove(path)

    def dumpToFile(self, cache):
        with open(self.createNewFile(), 'w') as f:
            f.write('\n'.join(sorted(cache, key=sortKey)))
