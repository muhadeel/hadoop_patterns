from mrjob.job import MRJob


class MRJobSum(MRJob):
    def mapper(self, _, line):
        # assume CSV input, format Author;Title;Year;Price
        attributes = line.split(';')
        yield attributes[2], 1

    def combiner(self, year, count):
        yield year, sum(count)

    def reducer(self, year, count):
        yield year, sum(count)


class MRJobAvg(MRJob):
    def mapper(self, _, line):
        # assume CSV input, format Author;Title;Year;Price
        attributes = line.split(';')
        yield attributes[2], attributes[3]

    def reducer(self, year, count):
        yield year, sum(count) / len(count)
       