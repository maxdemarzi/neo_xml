package com.maxdemarzi;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.test.server.HTTP;

import java.io.File;
import java.net.URL;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;
import static junit.framework.TestCase.assertEquals;

public class ImportTest {

    @Rule
    public final Neo4jRule neo4j = new Neo4jRule()
            .withProcedure(Import.class);

    @Test
    public void testImport() throws Exception {
        URL url = this.getClass().getResource("/data/xmlfeed.zip");
        File file = new File(url.getFile());
        Map query = singletonMap("statements",asList(singletonMap("statement",
                "CALL com.maxdemarzi.importXML('" + file.getAbsolutePath() + "')")));

        HTTP.Response response = HTTP.POST(neo4j.httpURI().resolve("/db/data/transaction/commit").toString(), query);
        String results = response.get("results").get(0).get("data").get(0).get("row").get(0).asText();
        assertEquals("File Imported in 0 seconds", results);
    }

}
