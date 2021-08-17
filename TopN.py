from mrjob.job import MRJob

N = 10


class MRWordTop10(MRJob):
    def mapper(self, _, line):
        # mapper yields the pair (studentID, grade) as a value
        attributes = line.split(';')
        studentId = int(attributes[0])
        grade = int(attributes[1])
        yield None, (studentId, grade)

    def reducer(self, _, pairs):
        # can be adapted for the combiner, instead of rank, yield grade
        # aggregation consists of sorting pairs and extracting top ten
        top10 = pairs.sort().getTop(N)
        rank = 1
        for student in top10:
            yield rank, student[0]
            rank += 1

    def mapper(self, _, line):
        try:
            col = line.split(",")
            symbol = col[1]
            date = col[2]
            amount = float(col[6]) * float(col[7])
            yield None, (amount, f'{symbol}_{date}')
        except:
            pass

    def reducer(self, key, values):
        topten = []
        for p in values:
            topten.append(p)

            # method 1
            topten.sort(reverse=True, key=lambda x: x[0])
            topten = topten[:10]

            # method 2:
            # topten.sort()
            # topten = topten[-10:]

        for p in topten:
            yield p[1], p[0]
