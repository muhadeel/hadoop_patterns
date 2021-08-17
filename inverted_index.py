from mrjob.job import MRJob


def find_features(text):
    return text.split('WHATEVER')


def formatNicely(docIds):
    # sort or something
    return docIds.sort()


class MRWordInvetedIndex(MRJob):
    def mapper(self, docId, text):
        features = find_features(text)
        for feature in features:
            yield feature, docId

    def reducer(self, feature, docIds):
        yield feature, formatNicely(docIds)
