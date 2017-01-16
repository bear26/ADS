import os
import random
import string
import shutil
import time

import functools

from Sorter import Sorter


def timer(func):
    @functools.wraps(func)
    def wrapper(filename):
        millis = int(round(time.time() * 1000))
        func(filename)
        print(int(round(time.time() * 1000) - millis), end='')

    return wrapper


def readStrs(filename):
    elements = []

    with open(filename, 'r') as f:
        for line in f:
            if not line:
                continue

            elements.append(line.strip(os.linesep))

    return elements


@timer
def inMemorySort(filename):
    elements = readStrs(filename)

    with open(filename, 'w') as f:
        for element in sorted(elements):
            f.write(str(element) + os.linesep)


@timer
def externalSort(filename):
    Sorter().sort(filename)


def generateFile(filename):
    size = 100000
    with open(filename, 'w') as f:
        for j in range(size):
            length = random.randint(200, 500)
            s = str()
            for i in range(length):
                s += random.choice(string.ascii_letters)
            f.write(s + os.linesep)


if __name__ == '__main__':
    name = 'file_test'
    filename = name + '.txt'
    filenameCopy = name + '_copy.txt'

    generateFile(filename)

    shutil.copyfile(filename, filenameCopy)

    print('In memory sort:', end='')
    inMemorySort(filenameCopy)
    print('ms')

    print('External sort', end='')
    externalSort(filename)
    print('ms')

    extSort = readStrs(filename)
    inMemorySort = readStrs(filenameCopy)

    os.remove(filenameCopy)
    os.remove(filename)

    assert extSort == inMemorySort, 'test Failed'

    print('Test passed')
