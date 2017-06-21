package com.maxdemarzi;

import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class Import {
    @Context
    public GraphDatabaseAPI db;

    @Context
    public Log log;

    @Description("com.maxdemarzi.importXML() ")
    @Procedure(name = "com.maxdemarzi.importXML", mode = Mode.WRITE)
    public Stream<StringResult> ImportXML(@Name("file") String file) throws InterruptedException {
        long start = System.nanoTime();
        Thread t1 = new Thread(new ImportXMLRunnable(file, db, log));
        t1.start();
        t1.join();

        return Stream.of(new StringResult("File Imported in " + TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start) + " seconds"));
    }

}
