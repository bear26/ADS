import os

import Settings


class Stream:
    def __init__(self, filename, chunkSize):
        self.file = open(filename, 'r')
        self.elements = []
        self.chunkSize = chunkSize
        self._readNextChunk()

    def __bool__(self):
        return bool(self.elements)

    def __lt__(self, other):
        return Settings.comparator(self.next(), other.next())

    def next(self):
        assert(len(self.elements) > 0)
        return self.elements[0]

    def pop(self):
        element = self.elements[0]
        self.elements.pop(0)

        if not self.elements:
            self._readNextChunk()

        return element

    def _readNextChunk(self):
        try:
            for i in range(self.chunkSize):
                line = self.file.readline()
                if line:
                    self.elements.append(line.strip(os.linesep))
        except EOFError as e:
            pass

    def close(self):
        if self.file:
            self.file.close()
