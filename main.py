from neo4j import GraphDatabase

uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "neo"))

def delete_all(tx):
    tx.run("match (n) with n DETACH DELETE n; ")

#______________AUTHORS______________#
def import_authors(tx):
    tx.run("LOAD CSV FROM \"file:///output_author.csv\" AS row FIELDTERMINATOR ';'\
            WITH row LIMIT 10000\
            CREATE (a:Author {authorID:row[0], name:row[1]});")
def constraint_authors(tx):
    tx.run("CREATE CONSTRAINT ON (n:Author) ASSERT n.authorID IS UNIQUE;")

#______________KEYWORDS_TOPICS______________#
def import_keywords_topics(tx):
    tx.run("LOAD CSV FROM \"file:///keyword_topic.csv\" AS row\
            CREATE (k:Keyword {keyword:row[0]})\
            MERGE (t:Topic {topic:row[1]})\
            CREATE (k)-[r:related_to]->(t);")
def constraint_keywords(tx):
    tx.run("CREATE CONSTRAINT ON (n:Keyword) ASSERT n.keyword IS UNIQUE;")

#______________ARTICLES_IN_CONFERENCES______________#
def import_articles_in_conferences(tx):
    tx.run("LOAD CSV FROM \"file:///articles_conferences.csv\" AS row\
            WITH row\
            WHERE row[0] IS NOT NULL\
            CREATE (a:Article {articleID:row[0], title:row[2], doi:row[3], abstract:row[5]})\
            MERGE (c:Conference {conferenceID: row[1]})\
            MERGE (y:Year {year:row[4]})\
            MERGE (e:Edition {editionID:row[6]})\
            MERGE (e)-[i:in]->(y)\
            MERGE (e)-[o:of]->(c)\
            CREATE (a)-[p:published_in]->(e)\
            WITH row, split(row[7], '|') as keywords , a\
            UNWIND keywords as term\
            MATCH (k:Keyword {keyword:term})\
            CREATE (a)-[h:has]->(k);")

def import_articles_in_journals(tx):
    tx.run("LOAD CSV WITH HEADERS FROM \"file:///articles_journals.csv\" AS line FIELDTERMINATOR ';'\
            CREATE(a:Article{articleID:line.articleID, title:line.title, DOI:line.doi, abstract:line.abstract, pages:toInteger(line.pages)})\
            MERGE(j:Journal{journalName:line.journal})\
            MERGE(v:Volume{volumeID:line.volumeID, volume:line.volume})\
            MERGE(y:Year{year:toInteger(line.year)})\
            CREATE(a)-[:published_in]->(v)\
            MERGE(v)-[:of]->(j)\
            MERGE(v)-[:in]->(y)\
            with split(line.keywords, '|') AS kws, a\
            UNWIND kws as kw\
            MATCH(k:Keyword{keyword:kw})\
            CREATE(a)-[:has]->(k)")

def constraint_articles(tx):
    tx.run("CREATE CONSTRAINT ON (n:Article) ASSERT n.articleID IS UNIQUE;")

#______________AUTHORED_BY______________#
def import_articles_authored_by(tx):
    tx.run("LOAD CSV FROM \"file:///output_author_authored_by.csv\" AS row FIELDTERMINATOR ';'\
            WITH row LIMIT 1000000\
            MATCH (au:Author {authorID:row[1]}), (a:Article {articleID:row[0]})\
            CREATE (au)-[w:write]->(a);")

#______________AUTHORED_BY______________#
def import_citations(tx):
    tx.run("LOAD CSV FROM \"file:///citations.csv\" AS row\
            WITH row\
            MATCH (a1:Article {articleID:row[0]}), (a2:Article {articleID:row[1]})\
            CREATE (a1)-[c:cite]->(a2);")

with driver.session() as session:

    # session.write_transaction(delete_all)
    #
    # session.write_transaction(import_authors)
    # print("finish loading author")
    #
    # session.write_transaction(constraint_authors)
    # print("finish indexing author")
    #
    # session.write_transaction(import_keywords_topics)
    # print("finish loading keywords_topics")
    # session.write_transaction(constraint_keywords)
    # print("finish indexing keywords")

    # session.write_transaction(import_articles_in_conferences)
    # print("finish loading articles_in_conferences")

    # session.write_transaction(import_articles_in_journals)
    # print("finish loading articles_in_journals")

    # session.write_transaction(constraint_articles)
    # print("finish indexing articles")

    session.write_transaction(import_articles_authored_by)
    print("finish loading authorships")

    # session.write_transaction(import_citations)
    # print("finish loading citations")








