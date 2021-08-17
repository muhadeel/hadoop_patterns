from mrjob.job import MRJob


# Replication join: Outer join, map-side.
# It can be implemented whenever one of the datasets is small enough to be kept in memory.
# During the initialisation of map tasks, the small dataset is replicated to all the nodes and cached in memory.
# Records from the large dataset are fed to mappers that look for a match in the second, small dataset kept in memory.

# Small dataset
# Dataset A:
# United States|US
# Canada|CA
# United Kingdom|UK
# Italy|IT

# Dataset B:
# Alice Bob|not bad|US
# Sam Sneed|valued|UK
# Jon Sneed|valued|CA
from mrjob.step import MRStep


class MRJobReplicationJoin(MRJob):
    def steps(self):
        return [MRStep(mapper_init=self.mapper_join_init,
                       mapper=self.mapper_replication_join,
                       reducer=self.reducer)]

    def mapper_join_init(self):
        # Re-define this to define an action to run before the mapper processes any input.
        # One use for this function is to initialize mapper-specific helper structures.
        #
        # Yields one or more tuples of (out_key, out_value).
        # By default, out_key and out_value must be JSON-encodable; re-define INTERNAL_PROTOCOL to change this.
        self.country_table = {}

        # load join_dataset_A into a dictionary
        # run the job with --file datasets/join_dataset_A.txt
        with open("/Users/hadeel/Project/ECS765P/hadoop_patterns/datasets/join_dataset_A.txt") as f:
            for line in f:
                splits = line.rstrip("\n").split("|")
                key = splits[1]
                val = splits[0]
                self.country_table[key] = val

    def mapper_replication_join(self, _, line):
        splits = line.rstrip("\n").split("|")
        country_symbol = str(splits[2])
        if self.country_table.get(country_symbol):
            dataset_A_row = [self.country_table[country_symbol], country_symbol]
            dataset_B_row = splits
            yield country_symbol, (dataset_A_row, dataset_B_row)

    def reducer(self, key, values):
        for value in values:
            yield key, value

if __name__ == '__main__':
    MRJobReplicationJoin.run()
