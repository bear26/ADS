import os

# how many elements we can hold in memory
sizeBlock = 10


class Stream:
    elements = []

    def __init__(self, path):
        self.file = open(path, 'r')
        self.elements = [int(x) for x in self.file.readline().split(' ') if x]

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
            self.elements = [int(x) for x in self.file.readline().split(' ') if x]
        except EOFError as e:
            pass

    def __bool__(self):
        return bool(self.elements)


class SorterHelper:
    streams = []

    def __init__(self, paths):
        self.comparator = lambda x1, x2: x1 < x2
        for path in paths:
            self.streams.append(Stream(path))

    def __iter__(self):
        while self.streams:
            minimalIdx = 0
            for i in range(len(self.streams)):
                if self.comparator(self.streams[i].next(), self.streams[minimalIdx].next()):
                    minimalIdx = i

            yield self.streams[minimalIdx].pop()

            if not self.streams[minimalIdx]:
                self.streams.pop(minimalIdx)


class Sorter:
    paths = []

    def sort(self, filename):
        self.splitAndSort(filename)

        # for debbugin
        filename += '_dest'

        with open(filename, 'w') as f:
            for element in SorterHelper(self.paths):
                f.write(str(element) + '\n')

        self.deleteTempFiles()

    def splitAndSort(self, filename):
        with open(filename, 'r') as f:
            cache = []
            for line in f:
                cache += [int(x) for x in line.split(' ')]
                if len(cache) > sizeBlock:
                    self.dumpToFile(cache)
                    cache.clear()

        self.dumpToFile(cache)

    def createNewFile(self):
        self.paths.append('__tempFileForSort' + str(len(self.paths)))
        return self.paths[-1]

    def deleteTempFiles(self):
        for path in self.paths:
            os.remove(path)

    def dumpToFile(self, cache):
        with open(self.createNewFile(), 'w') as f:
            for elem in sorted(cache):
                f.write(str(elem) + ' ')


def main():
    filename = 'file.txt'

    Sorter().sort(filename)


if __name__ == "__main__":
    main()