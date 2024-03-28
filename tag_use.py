# -*- coding: utf-8 -*-
from mrjob.job import MRJob
from mrjob.step import MRStep
import sys

class CountTagsUsage(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_tags,
                   reducer=self.reducer_count_tags)
        ]

    def mapper_get_tags(self, _, line):
        try:
            # Les colonnes du fichier tags.csv sont généralement séparées par des virgules
            user_id, movie_id, tag, timestamp = line.split(',')
            yield tag, 1  # Utilisez `tag` comme clé pour compter son occurrence
        except ValueError:
            # Cette exception est levée si la ligne n'a pas le bon nombre de valeurs
            # Ici, vous pouvez choisir de journaliser ou de passer
            pass
        except Exception as e:
            # Dans Python 2, utilisez sys.stderr.write au lieu de print pour écrire sur STDERR
            sys.stderr.write("Erreur inattendue: %s\n" % e)

    def reducer_count_tags(self, tag, counts):
        yield tag, sum(counts)  # Comptez et émettez le total des occurrences pour chaque tag

if __name__ == '__main__':
    CountTagsUsage.run()
