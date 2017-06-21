# neo_xml
POC XML Import for Neo4j

# Instructions

1. Build it:

        mvn clean package

2. Copy target/xml-import-1.0-SNAPSHOT.jar to the plugins/ directory of your Neo4j server.

3. Edit your Neo4j/conf/neo4j.conf file by adding this line:

        dbms.security.procedures.unrestricted=com.maxdemarzi.*

4. (Re)Start Neo4j server.

5. Call the procedure:

        CALL com.maxdemarzi.importXML('/Users/maxdemarzi/Projects/neo_xml/src/main/resources/data/xmlfeed.zip')
    
6. Check the nodes that were created:
    
        MATCH (n:Listing) RETURN n LIMIT 25
        
        