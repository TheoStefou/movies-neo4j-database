import csv
import time
import ast
from datetime import datetime

parsed_movies_metadata_columns = ['movie_id', 'tmdb_id', 'imdb_id', 'budget', 'title', 'original_language',
    'release_date', 'revenue', 'runtime', 'spoken_languages', 'tagline']

person_columns = ['id', 'name']

id_dict = {} #  tmpdb_id -> movie_id, imdb_id
with open('/vagrant/the-movies-dataset/links.csv') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    next(csv_reader, None)
    for index, row in enumerate(csv_reader):
        movie_id = row[0]
        imdb_id = row[1]
        tmdb_id = row[2]
        try:
            int(movie_id)
        except:
            print('id in line #{} is not an integer'.format(index))
            sys.exit()
        id_dict[tmdb_id] = (movie_id, imdb_id)

start = time.time()
print('Movies metadata...')
movie_ids = set()
with open('/vagrant/the-movies-dataset/movies_metadata.csv') as csv_file_read, open('/vagrant/the-movies-dataset/movies_metadata_parsed.csv', mode='w') as csv_file_write:
    csv_reader = csv.reader(csv_file_read, delimiter=',')
    csv_writer = csv.writer(csv_file_write, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    csv_writer.writerow(parsed_movies_metadata_columns)
    next(csv_reader, None)

    for index, row in enumerate(csv_reader):

        budget = row[2]
        if budget != '':
            try:
                int(budget)
            except:
                print('Record #{} error in budget field ({}). Setting to empty string.'.format(index, budget))
                budget = ''

        tmdb_id = row[5]
        try:
            int(tmdb_id)
        except:
            print('Record #{} error in tmdb_id field ({}). Setting to empty string.'.format(index, tmdb_id))
            tmpdb_id = ''

        imdb_id = row[6]
        if imdb_id != '' and (len(imdb_id) <= 2 or not (imdb_id[0], imdb_id[1]) == ('t', 't')):
            print('Record #{} error in imdb_id field. Must start with "tt" ({}). Setting to empty string.'.format(index, imdb_id))
            imdb_id = ''
        
        original_language = row[7]

        release_date = row[14]
        if release_date != '':
            try:
                datetime.strptime(release_date, '%Y-%m-%d')
            except:
                print('Record #{} error in release_date field ({}). Setting to empty string.'.format(index, release_date))
                release_date = ''

        revenue = row[15]
        if revenue != '':
            try:
                int(revenue)
            except:
                print('Record #{} error in revenue field ({}). Setting to empty string.'.format(index, revenue))
                revenue = ''

        runtime = row[16]
        if runtime != '':
            try:
                '''
                Values in csv count days, however they are all in float type
                resulting in integers appended with .0
                '''
                runtime = str(int(float(runtime)))
            except:
                print('Record #{} error in runtime field ({}). Setting to null.'.format(index, runtime))
                runtime = ''

        spoken_languages = row[17]
        if spoken_languages != '':
            try:
                spoken_languages = ast.literal_eval(spoken_languages)
                spoken_languages = str([language['iso_639_1'] for language in spoken_languages])
                spoken_languages = spoken_languages.replace('\'', '').replace(' ', '')
            except:
                print('Record #{} error in spoken_languages field ({}). Setting to empty string.'.format(index, spoken_languages))
                spoken_languages = ''

        tagline = row[19]

        title = row[20]

        movie_id = id_dict[tmdb_id][0]
        if movie_id in movie_ids:
            continue
        else:
            movie_ids.add(movie_id)
        csv_writer.writerow([movie_id, tmdb_id, imdb_id, budget, title, original_language, release_date, revenue, runtime, spoken_languages, tagline])
end = time.time()
print('Parsed movies_metadata.csv in {} seconds'.format(end - start))

print('Credits...')
start = time.time()
person_dict = {}
with open('/vagrant/the-movies-dataset/credits.csv') as csv_credits, open('/vagrant/the-movies-dataset/person.csv', mode='w') as csv_person, open('/vagrant/the-movies-dataset/cast.csv', mode='w') as csv_cast, open('/vagrant/the-movies-dataset/crew.csv', mode='w') as csv_crew:
    csv_credits_reader = csv.reader(csv_credits, delimiter=',')
    
    csv_person_writer = csv.writer(csv_person, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    csv_person_writer.writerow(person_columns)

    csv_cast_writer = csv.writer(csv_cast, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    csv_cast_writer.writerow(['person_id', 'movie_id', 'credit_id', 'character'])

    csv_crew_writer = csv.writer(csv_crew, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    csv_crew_writer.writerow(['person_id', 'movie_id', 'credit_id', 'job'])

    next(csv_credits_reader, None)

    for index, row in enumerate(csv_credits_reader):
        try:
            cast = ast.literal_eval(row[0])
        except:
            print('Record #{} error in cast field. Setting to empty list.'.format(index))
            cast = []

        try:
            crew = ast.literal_eval(row[1])
        except:
            print('Record #{} error in crew field. Setting to empty list.'.format(index))
            crew = []

        movie_id = id_dict[row[2]][0]

        for c in cast:
            person_dict[c['id']] = c['name']
            csv_cast_writer.writerow([c['id'], movie_id, c['credit_id'], c['character']])

        for c in crew:
            person_dict[c['id']] = c['name']
            csv_crew_writer.writerow([c['id'], movie_id, c['credit_id'], c['job']])

    for key,value in person_dict.items():
        csv_person_writer.writerow([key, value])
end = time.time()
print('Parsed credits.csv in {} seconds'.format(end - start))

