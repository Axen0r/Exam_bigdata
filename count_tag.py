# -*- coding: utf-8 -*-
from mrjob.job import MRJob
from mrjob.step import MRStep
import sys

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
        except ValueError:
            # Cette exception est levée si la ligne n'a pas le bon nombre de valeurs
            # Ici, vous pouvez choisir de journaliser ou de passer
            pass
        except Exception as e:
            # Dans Python 2, utilisez sys.stderr.write au lieu de print pour écrire sur STDERR
            sys.stderr.write("Erreur inattendue: %s\n" % e)

    def reducer_count_tags(self, movie_id, counts):
        yield movie_id, sum(counts)

if __name__ == '__main__':
    CountTagsByMovie.run()
