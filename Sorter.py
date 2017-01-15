import os
import heapq

import Settings
from Stream import Stream


class SorterHelper:
    def __init__(self, paths):
        self.streams = []
        for path in paths:
            heapq.heappush(self.streams, Stream(path, int(Settings.cacheSize / len(paths))))

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
                if len(cache) >= Settings.cacheSize:
                    self.dumpToFile(cache)
                    cache.clear()

    def createNewFile(self):
        self.paths.append('__tmpFile' + str(len(self.paths)))
        return self.paths[-1]

    def deleteTempFiles(self):
        for path in self.paths:
            os.remove(path)

    def dumpToFile(self, cache):
        with open(self.createNewFile(), 'w') as f:
            f.write('\n'.join(sorted(cache, key=Settings.sortKey)))
