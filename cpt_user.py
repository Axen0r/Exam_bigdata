# -*- coding: utf-8 -*-
from mrjob.job import MRJob
from mrjob.step import MRStep
import sys

class CountTagsByUser(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_tags_by_user,
                   reducer=self.reducer_count_tags)
        ]

    def mapper_get_tags_by_user(self, _, line):
        try:
            # Les colonnes du fichier tags.csv sont généralement séparées par des virgules
            user_id, movie_id, tag, timestamp = line.split(',')
            yield user_id, 1  # Utilise user_id comme clé
        except ValueError:
            # Cette exception est levée si la ligne n'a pas le bon nombre de valeurs
            # Ici, vous pouvez choisir de journaliser ou de passer
            pass
        except Exception as e:
            # Dans Python 2, utilisez sys.stderr.write au lieu de print pour écrire sur STDERR
            sys.stderr.write("Erreur inattendue: %s\n" % e)

    def reducer_count_tags(self, user_id, counts):
        # Somme tous les 1 pour chaque user_id, donnant le nombre total de tags ajoutés par l'utilisateur
        yield user_id, sum(counts)

if __name__ == '__main__':
    CountTagsByUser.run()
