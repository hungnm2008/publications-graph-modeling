from neo4j import GraphDatabase

uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "neo"))

def delete_all(tx):
    tx.run("MATCH (n:Meta) DETACH DELETE n;")

def create_meta_nodes(tx):
    tx.run("CREATE (m1:Meta {title:'Conference'})\
    CREATE (m2:Meta {title:'Edition'})\
    CREATE (m3:Meta {title:'Year'})\
    CREATE (m4:Meta {title:'Author'})\
    CREATE (m5:Meta {title:'Article'})\
    CREATE (m6:Meta {title:'Keyword'})\
    CREATE (m7:Meta {title:'Topic'})\
    CREATE (m8:Meta {title:'Volume'})\
    CREATE (m9:Meta {title:'Journal'})\
    RETURN m1,m2,m3,m4,m5,m6,m7,m8,m9")

def link_meta_nodes(tx):
    tx.run("MATCH (c:Meta {title:'Conference'}), (e:Meta {title:'Edition'})\
            CREATE (e)-[o:of]->(c)")
    tx.run("MATCH (y:Meta {title:'Year'}), (e:Meta {title:'Edition'})\
    CREATE (e)-[i:in]->(y)")
    tx.run("MATCH (a:Meta {title:'Article'}), (e:Meta {title:'Edition'})\
    CREATE (a)-[p:published_in]->(e)")
    tx.run("MATCH (a:Meta {title:'Article'}), (au:Meta {title:'Author'})\
    CREATE (au)-[w:write]->(a)")
    tx.run("MATCH (a:Meta {title:'Article'}), (au:Meta {title:'Author'})\
    CREATE (au)-[r:reviews]->(a)")
    tx.run("MATCH (a1:Meta {title:'Article'}), (a2:Meta {title:'Article'})\
    CREATE (a1)-[c:cites]->(a2)")
    tx.run("MATCH (a:Meta {title:'Article'}), (k:Meta {title:'Keyword'})\
    CREATE (a)-[h:has]->(k)")
    tx.run("MATCH (t:Meta {title:'Topic'}), (k:Meta {title:'Keyword'})\
    CREATE (k)-[r:related_to]->(t)")
    tx.run("MATCH (a:Meta {title:'Article'}), (v:Meta {title:'Volume'})\
    CREATE (a)-[p:published_in]->(v)")
    tx.run("MATCH (j:Meta {title:'Journal'}), (v:Meta {title:'Volume'})\
    CREATE (v)-[o:of]->(j)")
    tx.run("MATCH (y:Meta {title:'Year'}), (v:Meta {title:'Volume'})\
    CREATE (v)-[i:in]->(y)")

def create_data_nodes(tx):
    tx.run("MATCH (m:Meta {title:'Conference'})\
            CREATE (d:Data {title:'Text Understanding in LILOG'})-[i:isA]->(m)")
    tx.run("MATCH (m:Meta {title:'Year'})\
            CREATE (d:Data {title:'1991'})-[i:isA]->(m)")
    tx.run("MATCH (m:Meta {title:'Edition'})\
            CREATE (d:Data {title:'1991 Text Understanding in LILOG'})-[i:isA]->(m)")
    tx.run("MATCH (m:Meta {title:'Author'})\
            CREATE (d:Data {title:'Werner Emde'})-[i:isA]->(m)")
    tx.run("MATCH (m:Meta {title:'Author'})\
            CREATE (d:Data {title:'Raymond F. Boyce'})-[i:isA]->(m)")
    tx.run("MATCH (m:Meta {title:'Article'})\
            CREATE (d:Data {title:'Managing Lexical Knowledge in LEU/2.'})-[i:isA]->(m)")
    tx.run("MATCH (m:Meta {title:'Keyword'})\
            CREATE (d:Data {title:'data storage'})-[i:isA]->(m)")
    tx.run("MATCH (m:Meta {title:'Topic'})\
            CREATE (d:Data {title:'Computer Science'})-[i:isA]->(m)")
    tx.run("MATCH (m:Meta {title:'Article'})\
                CREATE (d:Data {title:'Object Data Language Facilities for Multimedia Data Types.'})-[i:isA]->(m)")
    tx.run("MATCH (m:Meta {title:'Volume'})\
                CREATE (d:Data {title:'GTE Laboratories Incorporated-TR-0169-12-91-165'})-[i:isA]->(m)")
    tx.run("MATCH (m:Meta {title:'Journal'})\
                CREATE (d:Data {title:'GTE Laboratories Incorporated'})-[i:isA]->(m)")

# def link_data_nodes(tx):
#     tx.run("MATCH (c:Meta {title:'Conference'}), (e:Meta {title:'Edition'})\
#             CREATE (e)-[o:of]->(c)")
#     tx.run("MATCH (y:Meta {title:'Year'}), (e:Meta {title:'Edition'})\
#     CREATE (e)-[i:in]->(y)")
#     tx.run("MATCH (a:Meta {title:'Article'}), (e:Meta {title:'Edition'})\
#     CREATE (a)-[p:published_in]->(e)")
#     tx.run("MATCH (a:Meta {title:'Article'}), (au:Meta {title:'Author'})\
#     CREATE (au)-[w:write]->(a)")
#     tx.run("MATCH (a:Meta {title:'Article'}), (au:Meta {title:'Author'})\
#     CREATE (au)-[r:reviews]->(a)")
#     tx.run("MATCH (a1:Meta {title:'Article'}), (a2:Meta {title:'Article'})\
#     CREATE (a1)-[c:cites]->(a2)")
#     tx.run("MATCH (a:Meta {title:'Article'}), (k:Meta {title:'Keyword'})\
#     CREATE (a)-[h:has]->(k)")
#     tx.run("MATCH (t:Meta {title:'Topic'}), (k:Meta {title:'Keyword'})\
#     CREATE (k)-[r:related_to]->(t)")
#     tx.run("MATCH (a:Meta {title:'Article'}), (v:Meta {title:'Volume'})\
#     CREATE (a)-[p:published_in]->(v)")
#     tx.run("MATCH (j:Meta {title:'Journal'}), (v:Meta {title:'Volume'})\
#     CREATE (v)-[o:of]->(j)")
#     tx.run("MATCH (y:Meta {title:'Year'}), (v:Meta {title:'Volume'})\
#     CREATE (v)-[i:in]->(y)")

with driver.session() as session:
    session.write_transaction(delete_all)
    session.write_transaction(create_meta_nodes)
    session.write_transaction(link_meta_nodes)
    session.write_transaction(create_data_nodes)

#
# CREATE META NODES
# CREATE (c:Meta {title:'Conference'})
# CREATE (c:Meta {title:'Edition'})
# CREATE (c:Meta {title:'Year'})
# CREATE (c:Meta {title:'Author'})
# CREATE (c:Meta {title:'Article'})
# CREATE (c:Meta {title:'Keyword'})
# CREATE (c:Meta {title:'Topic'})
# CREATE (c:Meta {title:'Volume'})
# CREATE (c:Meta {title:'Journal'})

#
# LINK META NODES
# MATCH (c:Meta {title:'Conference'}), (e:Meta {title:'Edition'})
# CREATE (e)-[o:of]->(c)
# MATCH (y:Meta {title:'Year'}), (e:Meta {title:'Edition'})
# CREATE (e)-[i:in]->(y)
# MATCH (a:Meta {title:'Article'}), (e:Meta {title:'Edition'})
# CREATE (a)-[p:published_in]->(e)
# MATCH (a:Meta {title:'Article'}), (au:Meta {title:'Author'})
# CREATE (au)-[w:write]->(a)
# MATCH (a:Meta {title:'Article'}), (au:Meta {title:'Author'})
# CREATE (au)-[r:reviews]->(a)
# MATCH (a1:Meta {title:'Article'}), (a2:Meta {title:'Article'})
# CREATE (a1)-[c:cites]->(a2)
# MATCH (a:Meta {title:'Article'}), (k:Meta {title:'Keyword'})
# CREATE (a)-[h:has]->(k)
# MATCH (t:Meta {title:'Topic'}), (k:Meta {title:'Keyword'})
# CREATE (k)-[r:related_to]->(t)
# MATCH (a:Meta {title:'Article'}), (v:Meta {title:'Volume'})
# CREATE (a)-[p:published_in]->(v)
# MATCH (j:Meta {title:'Journal'}), (v:Meta {title:'Volume'})
# CREATE (v)-[o:of]->(j)
# MATCH (y:Meta {title:'Year'}), (v:Meta {title:'Volume'})
# CREATE (v)-[i:in]->(y)
#
#
# # CREATE DATA NODES
# CREATE (c:Meta {title:'Conference'})
# CREATE (c:Meta {title:'Edition'})
# CREATE (c:Meta {title:'Year'})
# CREATE (c:Meta {title:'Author'})
# CREATE (c:Meta {title:'Article'})
# CREATE (c:Meta {title:'Keyword'})
# CREATE (c:Meta {title:'Topic'})
# CREATE (c:Meta {title:'Volume'})
# CREATE (c:Meta {title:'Journal'})