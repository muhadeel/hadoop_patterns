from mrjob.job import MRJob


# Repartition join: Reduce-side, for large datasets.
# Both datasets are processed by the mappers, that emit the joining attribute as the key,
# the value includes all the other attributes that you are interested in.
# All the emitted key/values that have the same key correspond to pieces of data
# from different datasets that we want to combine.
# Reducers combine them together.

# Dataset A:
# United States|US
# Canada|CA
# United Kingdom|UK
# Italy|IT

# Dataset B:
# Alice Bob|not bad|US
# Sam Sneed|valued|UK
# Jon Sneed|valued|CA

class MRJobRepartitionJoin(MRJob):
    # Performs secondary sort
    # essential step to get first data from dataset A in the reducer
    SORT_VALUES = True

    def mapper(self, _, line):
        # the line could be from dataset A or dataset B
        splits = line.rstrip("\n").split("|")

        if len(splits) == 2:  # country data
            symbol = 'A'  # make country sort before person data
            country_symbol = splits[1]
            # UK, (A, [United Kingdom, UK])
            yield country_symbol, (symbol, splits)
        else:  # person data
            symbol = 'B'
            country_symbol = splits[2]
            # UK, (B, [Sam Sneed, valued, UK])
            # UK, (B, [Alex Ball, valued, |UK)
            yield country_symbol, (symbol, splits)

    def reducer(self, country_symbol, values):
        values = list(values)
        dataset_A_row = []
        dataset_B_row = []
        for value in values:
            if value[0] == 'A':
                # there will be one row in dataset A for each country_symbol
                dataset_A_row = value[1]
            if value[0] == 'B':
                # there will be multiple rows in dataset B for each country_symbol
                dataset_B_row = value[1]

            # doing inner join, both datasets rows must be available
            if dataset_A_row and dataset_B_row:
                yield country_symbol, dataset_A_row + dataset_B_row


if __name__ == '__main__':
    MRJobRepartitionJoin.run()



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