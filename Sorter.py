import os
import heapq

import Settings
from FileReader import Stream, FileReader


class SorterHelper:
    def __init__(self, paths):
        self.streams = []
        for path in paths:
            heapq.heappush(self.streams, Stream(path, Settings.cacheSize / len(paths)))

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
        fileReader = FileReader(filename)

        while fileReader:
            fileReader.resetCacheSize(Settings.cacheSize)

            cache = []

            while True:
                line = fileReader.readline()

                if not line:
                    break

                cache.append(line)

            self.dumpToFile(cache)
            cache.clear()

        fileReader.close()

    def createNewFile(self):
        self.paths.append('__tempFileForSort' + str(len(self.paths)))
        return self.paths[-1]

    def deleteTempFiles(self):
        for path in self.paths:
            os.remove(path)

    def dumpToFile(self, cache):
        with open(self.createNewFile(), 'w') as f:
            f.write('\n'.join(sorted(cache, key=Settings.sortKey)))
