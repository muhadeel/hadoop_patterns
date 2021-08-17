import re

from mrjob.job import MRJob


class MRJobFilter1(MRJob):
    def mapper(self, _, line):
        # assume CSV input, format Author;Title;Year
        # retrieve records from 2021
        attributes = line.split(';')
        if attributes[2] == '2021':
            yield line, None

    def reducer(self, key, values):
        yield key, None


class MRJobFilter2(MRJob):
    def mapper(self, _, line):
        # sherlock.txt input
        # retrieve lines that contain the word London
        # uses regular expression package
        if re.search('London', line):
            yield line, None

    def reducer(self, key, values):
        yield key, None
