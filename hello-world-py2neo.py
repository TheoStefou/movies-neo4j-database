from py2neo import Graph
from py2neo import Node
from py2neo import Relationship

graph = Graph(password="m222")
graph.delete_all()

nicole = Node("Person", name="Nicole", age=24)
drew = Node("Person", name="Drew", age=20)

mtdew = Node("Drink", name="Mountain Dew", calories=9000)
cokezero = Node("Drink", name="Coke Zero", calories=0)

coke = Node("Manufacturer", name="Coca Cola")
pepsi = Node("Manufacturer", name="Pepsi")

graph.create(nicole | drew | mtdew | cokezero | coke | pepsi)

graph.create(Relationship(nicole, "LIKES", cokezero))
graph.create(Relationship(nicole, "LIKES", mtdew))
graph.create(Relationship(drew, "LIKES", mtdew))
graph.create(Relationship(coke, "MAKES", cokezero))
graph.create(Relationship(pepsi, "MAKES", mtdew))

query = """
MATCH (person:Person)-[:LIKES]->(drink:Drink)
RETURN person.name AS name, drink.name AS drink
"""

data = graph.run(query)
print("\n\nPeople who like a drink")
for d in data:
    print(d)

query = """
MATCH (p:Person)-[:LIKES]->(drink:Drink)
WHERE p.name = $name AND drink.name = $drink
RETURN p.name AS name, AVG(drink.calories) AS avg_calories
"""

data = graph.run(query, name="Nicole", drink="Coke Zero")
print("\n\nNodes (and calories) with name 'Nicole' that like 'Coke Zero'")
for d in data:
    print(d)

