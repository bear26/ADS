import heapq

import Settings
from Stream import Stream


class SorterHelper:
    def __init__(self, paths):
        maxFileMerged = Settings.cacheSize - 1

        while len(paths) > maxFileMerged:
            newPaths = []
            while len(paths) > maxFileMerged:
                pathsChunk = []
                for i in range(maxFileMerged):
                    pathsChunk.append(paths.pop())

                newPaths.append(self._sort(pathsChunk))

            if paths:
                newPaths.append(self._sort(paths))

            paths = newPaths

        self.streams = []
        for path in paths:
            heapq.heappush(self.streams, Stream(path, int(Settings.cacheSize / len(paths))))

    def _sort(self, paths):
        name = paths[0] + 'n'
        with open(name, 'w') as f:
            for element in SorterHelper(paths):
                f.write(str(element) + '\n')

        return name

    def __iter__(self):
        while self.streams:
            smallest = heapq.heappop(self.streams)

            yield smallest.pop()

            if smallest:
                heapq.heappush(self.streams, smallest)
            else:
                smallest.closeAndRemove()


class Sorter:
    def sort(self, filename):
        self.paths = []
        self.splitAndSort(filename)

        with open(filename, 'w') as f:
            for elem in SorterHelper(self.paths):
                f.write(elem + '\n')

    def splitAndSort(self, filename):
        with open(filename, 'r') as f:
            cache = []
            for line in f:
                line = line.strip('\n')
                if not line:
                    continue

                cache.append(line)
                if len(cache) >= Settings.cacheSize:
                    self.dumpToFile(cache)
                    cache.clear()

            if cache:
                self.dumpToFile(cache)
                cache.clear()

    def createNewFile(self):
        self.paths.append('_t' + str(len(self.paths)))
        return self.paths[-1]

    def dumpToFile(self, cache):
        with open(self.createNewFile(), 'w') as f:
            for element in sorted(cache, key=Settings.sortKey):
                f.write(element + '\n')
