from mrjob.job import MRJob

N = 10


class MRTop100(MRJob):
    def mapper(self, _, line):
        # mapper yields the pair (studentID, grade) as a value
        attributes = line.split(';')
        studentId = getStudentId
        grade = getGrade

        if hadGraduted:
            yield studentId, grade

    def reducer1(self, key, values):
        avg = sum(values)/ len(values)
        yield None, (key, avg)

    def reducer2(self, _, pairs):
        top10 = pairs.sort().getTop(100)
        rank = 1
        for student in top10:
            yield rank, student[0]
            rank += 1



    def mapper(self, _, line):
        splits = line.rstrip("\n").split("|")

        if len(splits) == 6:  # citizen data
            symbol = 'A'  # make country sort before person data
            citizenId = splits[0]
            sugeryId = splits[1]
            yield citizenId, [symbol, sugeryId]
        else:  # person data
            symbol = 'B'
            citizenId = splits[1]
            # date, appointment
            appointment_data = splits[3:]

            yield citizenId, [symbol, appointment_data]

    def reducer(self, citizenId, values):
        citizens = []  # should come first, as they are sorted on artificial key 'A’

        for value in values:
            if value[0] == 'A':
                citizens.append(value)
            if value[0] == 'B':
                for citizen in citizens:
                    surgery_id = citizen[1]
                    appointment_data = value[1]
                    yield surgery_id, (citizenId, appointment_data)