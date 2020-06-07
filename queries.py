from py2neo import Graph
from string import Template

graph = Graph(password='m222')

def escape(s):
    return s.replace('\"', '\\"').replace('\'', '\\\'')

def id_from_name(name):
    
    global graph
    name = escape(name)
    query = Template('''
    MATCH (p:Person)
    WHERE p.name = '$name'
    RETURN p.id AS id
    ''').substitute(name=name)

    return graph.run(query).data()

def query1(person_id):

    global graph
    query = Template('''
    MATCH (p:Person)-[r:CAST]->(m:Movie)
    WHERE p.id = $person_id
    RETURN DISTINCT m.title AS title, m.tagline AS tagline, m.release_date AS release_date
    ''').substitute(person_id=person_id)

    return graph.run(query).data()

def query2_director(person_id):

    global graph
    query = Template('''
    MATCH (p:Person)-[r:CREW]->(m:Movie)
    WHERE p.id = $person_id AND r.job = 'Director'
    RETURN m.title AS title, m.tagline AS tagline, m.release_date AS release_date
    ''').substitute(person_id=person_id)

    return graph.run(query).data()

def query2_writer(person_id):

    global graph
    query = Template('''
    MATCH (p:Person)-[r:CREW]->(m:Movie)
    WHERE p.id = $person_id AND r.job = 'Writer'
    RETURN m.title AS title, m.tagline AS tagline, m.release_date AS release_date
    ''').substitute(person_id=person_id)

    return graph.run(query).data()

def query2_producer(person_id):

    global graph
    query = Template('''
    MATCH (p:Person)-[r:CREW]->(m:Movie)
    WHERE p.id = $person_id AND r.job = 'Producer'
    RETURN m.title AS title, m.tagline AS tagline, m.release_date AS release_date
    ''').substitute(person_id=person_id)

    return graph.run(query).data()

def query3(person_id, fr, to):

    global graph
    query = Template('''
    MATCH (p:Person)-[r:CAST]->(m:Movie)
    WHERE p.id = $person_id AND m.release_date >= date("$fr") AND m.release_date <= date("$to")
    RETURN r.character AS character
    ''').substitute(person_id=person_id, fr=fr, to=to)

    return graph.run(query).data()

def query4_original(person_id):

    global graph
    query = Template('''
    MATCH (p:Person)-[r:CAST]->(m:Movie)
    WHERE p.id = $person_id
    RETURN DISTINCT m.original_language AS original_language
    ''').substitute(person_id=person_id)

    return graph.run(query).data()

def query4_spoken(person_id):

    global graph
    query = Template('''
    MATCH (p:Person)-[r:CAST]->(m:Movie)
    WHERE p.id = $person_id
    UNWIND m.spoken_languages as spoken
    RETURN DISTINCT spoken AS spoken_language
    ''').substitute(person_id=person_id)

    return graph.run(query).data()

def query5(top_k):

    global graph
    query = Template('''
    MATCH (p:Person)-[r:CREW]->(m:Movie)
    WHERE r.job = 'Director' AND EXISTS(m.runtime)
    WITH p.id AS director_id, p.name AS director_name, AVG(m.runtime) as average_runtime
    ORDER BY average_runtime DESC
    RETURN director_id, director_name, average_runtime
    LIMIT $top_k
    ''').substitute(top_k=top_k)

    return graph.run(query).data()

def query6_budget(top_k):

    global graph
    query = Template('''
    MATCH (p:Person)-[r:CREW]->(m:Movie)
    WHERE r.job = 'Producer' AND EXISTS(m.budget)
    WITH p.id AS producer_id, p.name AS producer_name, MAX(m.budget) as max_budget
    ORDER BY max_budget DESC
    RETURN producer_id, producer_name, max_budget
    LIMIT $top_k
    ''').substitute(top_k=top_k)

    return graph.run(query).data()

def query6_revenue(top_k):

    global graph
    query = Template('''
    MATCH (p:Person)-[r:CREW]->(m:Movie)
    WHERE r.job = 'Producer' AND EXISTS(m.revenue)
    WITH p.id AS producer_id, p.name AS producer_name, MAX(m.revenue) as max_revenue
    ORDER BY max_revenue DESC
    RETURN producer_id, producer_name, max_revenue
    LIMIT $top_k
    ''').substitute(top_k=top_k)

    return graph.run(query).data()

def query7(year):

    global graph
    query = Template('''
    MATCH (p1:Person)-[:CAST]->(m1:Movie)<-[:CAST]-(p2:Person), (p1)-[:CAST]->(m2:Movie)<-[:CAST]-(p2)
    WHERE p1.id < p2.id AND m1 <> m2 AND m1.release_date.year = $year AND m2.release_date.year = $year
    RETURN DISTINCT p1.id AS id1, p1.name AS name1, p2.id AS id2, p2.name AS name2
    ''').substitute(year=year)

    return graph.run(query).data()

def query8(year):

    global graph
    query = Template('''
    MATCH (p1:Person)-[r1:CREW]->(m1:Movie), (p1)-[r2:CREW]->(m2:Movie)
    WHERE r1.job = 'Director' AND r2.job = 'Producer' AND m1.release_date.year = $year AND m2.release_date.year = $year
    RETURN DISTINCT p1.id AS id, p1.name AS name
    ''').substitute(year=year)

    return graph.run(query).data()

def query9(year):

    global graph
    query = Template('''
    MATCH (p1:Person)-[r1:CREW]->(m1:Movie), (p1)-[r2:CREW]->(m2:Movie), (p1)-[:CAST]->(m3:Movie)
    WHERE r1.job = 'Director' AND r2.job = 'Writer' AND m1.release_date.year = $year AND m2.release_date.year = $year AND m3.release_date.year = $year
    RETURN DISTINCT p1.id AS id, p1.name AS name
    ''').substitute(year=year)

    return graph.run(query).data()

def query10(person_id):

    global graph
    query = Template('''
    MATCH (p1:Person)-[:CAST]->(m:Movie)<-[:CAST]-(p2:Person)
    WHERE p1.id = $person_id AND p1 <> p2
    WITH DISTINCT p2
    MATCH (p2)-[:CAST]->(m:Movie)<-[:CAST]-(p:Person)
    WHERE p.id <> $person_id AND p2 <> p
    WITH DISTINCT p
    MATCH (p1:Person {id: $person_id})
    WHERE NOT (p)-[:CAST]->(:Movie)<-[:CAST]-(p1)
    RETURN p.id AS id
    ''').substitute(person_id=person_id)

    return graph.run(query).data()


def query11(person_id):

    global graph
    query = Template('''
    MATCH (p1:Person)-[:CREW {job: 'Director'}]->(:Movie)<-[:CAST]- (p2:Person {id: $person_id})
    WHERE p1 <> p2
    RETURN DISTINCT p1.id AS id, p1.name AS name
    ''').substitute(person_id=person_id)

    return graph.run(query).data()

def query12(person_id, top_k):

    global graph
    query = Template('''
    MATCH (p1:Person), (p2:Person {id: $person_id})
    WHERE NOT EXISTS ((p1)-[:CREW {job:'Director'}]->(:Movie)<-[:CAST]-(p2))
    MATCH (p1)-[:CREW {job: 'Director'}]->(:Movie)<-[:CAST]-(p3:Person)
    WHERE EXISTS ((p2)-[:CAST]->(:Movie)<-[:CAST]-(p3))
    RETURN p1.id AS id, p1.name AS name, COUNT(p3) AS num
    ORDER BY num DESC
    LIMIT $top_k
    ''').substitute(person_id=person_id, top_k=top_k)

    return graph.run(query).data()

def query13():

    global graph
    query = '''
    MATCH (p1:Person)-[r1:CREW]->(m1:Movie)<-[:CAST]-(p2:Person), (p2)-[r2:CREW]->(m2:Movie)<-[:CAST]-(p1)
    WHERE r1.job = 'Director' AND r2.job = 'Director' AND p1.id < p2.id
    RETURN DISTINCT p1.id AS id1, p1.name AS name1, p2.id AS id2, p2.name AS name2
    '''

    return graph.run(query).data()

def query14(years):

    global graph
    query = Template('''
    MATCH (p:Person)-[:CREW {job : 'Director'}]->(m:Movie)
    WITH p, m.release_date AS release_date
    ORDER BY release_date
    WITH p, collect(release_date) as consecutive_dates
    WITH p, [i in range(0, size(consecutive_dates)-2) WHERE duration.between(consecutive_dates[i], consecutive_dates[i+1]).years >= $years | i] AS result
    WHERE size(result) > 0
    RETURN p.id AS id, p.name AS name
    ''').substitute(years=years)

    return graph.run(query).data()


def query15():

    global graph
    query = '''
    MATCH (m:Movie)-[r:RATED_BY]->(u:User)
    RETURN m.movie_id AS movie_id, m.title AS title, count(r) AS num_votes
    ORDER BY num_votes DESC
    LIMIT 1
    '''

    return graph.run(query).data()

def query16_best():

    global graph
    query = '''
    MATCH (m:Movie)-[r:RATED_BY]->(u:User)
    RETURN m.movie_id AS movie_id, m.title AS title, AVG(r.rating) AS avg_rating
    ORDER BY avg_rating DESC
    LIMIT 1 
    '''

    return graph.run(query).data()

def query16_worst():

    global graph
    query = '''
    MATCH (m:Movie)-[r:RATED_BY]->(u:User)
    RETURN m.movie_id AS movie_id, m.title AS title, AVG(r.rating) AS avg_rating
    ORDER BY avg_rating
    LIMIT 1 
    '''

    return graph.run(query).data()

    
def query17_best():

    global graph
    query = '''
    MATCH (m:Movie)-[r:RATED_BY]->(u:User)
    WITH m AS mov, AVG(r.rating) AS avg_rating
    MATCH (p:Person)-[r:CREW]->(mov)
    WHERE r.job = 'Director'
    RETURN p.id AS id, p.name AS name, AVG(avg_rating) as director_avg_rating
    ORDER BY director_avg_rating DESC
    LIMIT 1
    '''

    return graph.run(query).data()

def query17_worst():

    global graph
    query = '''
    MATCH (m:Movie)-[r:RATED_BY]->(u:User)
    WITH m AS mov, AVG(r.rating) AS avg_rating
    MATCH (p:Person)-[r:CREW]->(mov)
    WHERE r.job = 'Director'
    RETURN p.id AS id, p.name AS name, AVG(avg_rating) as director_avg_rating
    ORDER BY director_avg_rating
    LIMIT 1
    '''

    return graph.run(query).data()

def query18():

    global graph
    query = '''
    MATCH (m:Movie)-[r:RATED_BY]->(:User)
    WITH m, COUNT(r) AS num_votes
    MATCH (p1:Person)-[:CAST]->(m)<-[:CAST]-(p2:Person)
    WHERE p1.id < p2.id
    WITH p1, p2, m, num_votes
    RETURN p1.id, p1.name, p2.id, p2.name, SUM(num_votes) AS total_votes
    ORDER BY total_votes DESC
    LIMIT 1
    '''

    return graph.run(query).data()
