from neo4j import GraphDatabase

uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "neo"))

def delete_all(tx):
    tx.run("match (n) with n DETACH DELETE n; ")

#______________AUTHORSHIPS______________#
def import_authors(tx):
    tx.run("LOAD CSV WITH HEADERS FROM \"file:///authors.csv\" AS row FIELDTERMINATOR ','\
            WITH row\
            CREATE (a:Author {authorID:row.authorID, name:row.authorName});")
def constraint_authors(tx):
    tx.run("CREATE CONSTRAINT ON (n:Author) ASSERT n.authorID IS UNIQUE;")
def import_articles_authored_by(tx):
    tx.run("LOAD CSV FROM \"file:///authored_by.csv\" AS row\
            MATCH (au:Author {authorID:row[1]}), (a:Article {articleID:row[0]})\
            CREATE (au)-[w:write]->(a);")

#______________KEYWORDS_TOPICS______________#
def import_keywords_topics(tx):
    tx.run("LOAD CSV FROM \"file:///keyword_topic.csv\" AS row\
            CREATE (k:Keyword {keyword:row[0]})\
            MERGE (t:Topic {topic:row[1]})\
            CREATE (k)-[r:related_to]->(t);")
def constraint_keywords(tx):
    tx.run("CREATE CONSTRAINT ON (n:Keyword) ASSERT n.keyword IS UNIQUE;")

#______________ARTICLES______________#
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
            MERGE(j:Journal{journalID:line.journal})\
            MERGE(v:Volume{volumeID:line.volumeID})\
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

#______________CITATIONS______________#
def import_citations(tx):
    tx.run("LOAD CSV WITH HEADERS FROM \"file:///citations.csv\" AS row FIELDTERMINATOR ';'\
            WITH row\
            MATCH (a:Article {articleID:row.articleID})\
            WITH split(row.cites, '|') AS cited_articles, a\
            UNWIND cited_articles as cited_article\
            MATCH(c:Article{articleID:cited_article})\
            CREATE (a)-[ci:cites]->(c);")

#______________REVIEW______________#
def import_reviewed_by(tx):
    tx.run("LOAD CSV WITH HEADERS FROM \"file:///reviewed_by.csv\" AS row FIELDTERMINATOR ';'\
            WITH row\
            MATCH (a:Article {articleID:row.articleID})\
            WITH split(row.reviewerID, '|') AS reviewers, a\
            UNWIND reviewers as reviewer\
            MATCH(au:Author{authorID:reviewer})\
            CREATE (au)-[r:reviews]->(a);")

#____________AFFILIATION(as nodes)____________#
def import_affiliations_(tx):
    tx.run("LOAD CSV WITH HEADERS FROM \"file:///authors.csv\" AS row\
            WITH row\
            MATCH (au:Author{authorID:row.authorID})\
            MERGE(o:Organisation{orgName:row.orgName, orgType:row.orgType})\
            CREATE(au)-[a:affiliated_with]->(o);")

#____________AFFILIATION(as properties)____________#
def import_affiliations(tx):
    tx.run("LOAD CSV WITH HEADERS FROM \"file:///authors.csv\" AS row\
            WITH row\
            MATCH (au:Author{authorID:row.authorID})\
            SET au.organisation = row.orgName, au.orgType=row.orgType;")

#______________MODIFYING REVIEWS EDGE_____________#
def modify_reviews(tx):
    tx.run("LOAD CSV WITH HEADERS FROM \"file:///reviewed_by_decision.csv\" AS row FIELDTERMINATOR ';'\
            WITH split(row.review1, '|') AS review1_info,\
            split(row.review2, '|') AS review2_info,\
            split(row.review3, '|') AS review3_info, row\
            MATCH (a:Article{articleID:row.articleID}),\
            (au1:Author{authorID: review1_info[0]}),\
            (au2:Author{authorID: review2_info[0]}),\
            (au3:Author{authorID: review3_info[0]})\
            MATCH(au1)-[r1:reviews]->(a)\
            MATCH(au2)-[r2:reviews]->(a)\
            MATCH (au3)-[r3:reviews]->(a)\
            SET r1.review = review1_info[1]\
            SET r1.decision = review1_info[2]\
            SET r2.review = review2_info[1]\
            SET r2.decision = review2_info[2]\
            SET r3.review = review3_info[1]\
            SET r3.decision = review3_info[2];")


with driver.session() as session:

    session.write_transaction(delete_all)

    session.write_transaction(import_authors) #1000 rows
    print("finished loading authors")

    session.write_transaction(constraint_authors)
    print("finished indexing authors")
    
    session.write_transaction(import_keywords_topics)
    print("finished loading keywords_topics")

    session.write_transaction(constraint_keywords)
    print("finished indexing keywords")

    session.write_transaction(import_articles_in_conferences) #50 000 rows
    print("finished loading articles_in_conferences")

    session.write_transaction(import_articles_in_journals) #50 000 rows
    print("finished loading articles_in_journals")

    session.write_transaction(constraint_articles)
    print("finished indexing articles")
    
    session.write_transaction(import_articles_authored_by) #500 000 rows
    print("finished loading authorships")

    session.write_transaction(import_citations) #100 000 rows
    print("finished loading citations")

    session.write_transaction(import_reviewed_by) #10 000 rows
    print("finished loading reviewed_by")

    session.write_transaction(import_affiliations)
    print("finished loading affiliations")

    session.write_transaction(modify_reviews) #10 000 rows
    print("finished loading modified reviews")
    








