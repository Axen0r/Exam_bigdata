from mrjob.job import MRJob
from mrjob.step import MRStep

class CountTagsByMovie(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_tags,
                   reducer=self.reducer_count_tags)
        ]

    def mapper_get_tags(self, _, line):
        try:
            # Les colonnes du fichier tags.csv sont généralement séparées par des virgules
            user_id, movie_id, tag, timestamp = line.split(',')
            yield movie_id, 1
        except Exception:
            pass

    def reducer_count_tags(self, movie_id, counts):
        yield movie_id, sum(counts)

if __name__ == '__main__':
    CountTagsByMovie.run()
