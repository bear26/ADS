import Settings


class FileReader:
    def __init__(self, filename):
        self.file = open(filename, 'r')
        self.buffer = str()
        self.isEof = False
        self.totalByteRead = 0
        self.maximumCacheSize = 0

    def resetCacheSize(self, size):
        self.totalByteRead = len(self.buffer)
        self.maximumCacheSize = size

    def availableSize(self):
        return self.maximumCacheSize - self.totalByteRead

    def readline(self,):
        assert len(self.buffer) < self.availableSize(), 'Cache size must be greater than size of line'

        while self.availableSize() > 0:
            c = self.file.read(1)

            if not c:
                self.isEof = True
                return self.returnLine()

            if c == '\n':
                # skip empty line
                if not len(self.buffer):
                    continue
                else:
                    return self.returnLine()

            self.buffer += c
            self.totalByteRead += len(c)

        # not enough cache size for read line
        return []

    def returnLine(self):
        line = self.buffer
        self.buffer = str()
        return str(line)

    def __bool__(self):
        return not self.isEof

    def close(self):
        assert len(self.buffer) == 0, 'not all data read'
        self.file.close()


class Stream:
    def __init__(self, filename, chunkSize):
        self.fileReader = FileReader(filename)
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
        self.fileReader.resetCacheSize(self.chunkSize)

        while True:
            line = self.fileReader.readline()
            if not line:
                break

            self.elements.append(line)

    def close(self):
        if self.fileReader:
            self.fileReader.close()
