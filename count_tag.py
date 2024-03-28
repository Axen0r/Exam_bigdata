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
        except ValueError:
            # Cette exception est levée si la ligne n'a pas le bon nombre de valeurs
            # Ici, vous pouvez choisir de journaliser ou de passer
            pass
        except Exception as e:
            # Journaliser les autres types d'erreurs pour révision.
            # Il est important de noter que l'écriture sur STDERR peut ne pas être capturée par certains environnements d'exécution Hadoop
            # Alternativement, envisagez d'utiliser un système de journalisation ou de marquer les erreurs d'une manière qui convient à votre environnement.
            print(f"Erreur inattendue: {e}", file=sys.stderr)

    def reducer_count_tags(self, movie_id, counts):
        yield movie_id, sum(counts)

if __name__ == '__main__':
    CountTagsByMovie.run()
