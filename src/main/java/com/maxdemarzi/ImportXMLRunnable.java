package com.maxdemarzi;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static javax.xml.stream.XMLStreamConstants.*;

public class ImportXMLRunnable implements Runnable {
    private static final int TRANSACTION_LIMIT = 1000;
    private String file;
    private GraphDatabaseAPI db;
    private Log log;


    public ImportXMLRunnable(String file, GraphDatabaseAPI db, Log log) {
        this.file = file;
        this.db = db;
        this.log = log;
    }

    @Override
    public void run() {
        long start = System.nanoTime();

        Date time = Calendar.getInstance().getTime();
        long updated_at = time.getTime();

        ZipFile zip;

        try {
            zip = new ZipFile(new File(file));
            ZipEntry entry = zip.entries().nextElement();
            InputStream xmlStream = zip.getInputStream(entry);
            XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
            XMLStreamReader xmlStreamReader = xmlInputFactory.createXMLStreamReader(xmlStream);

            ArrayList<HashMap<String, Object>> listings = new ArrayList<>();
            int counter = 1;

            int event = xmlStreamReader.getEventType();
            while (true) {
                switch (event) {
                    case START_ELEMENT:
                        if (xmlStreamReader.getLocalName().equals("listing")) {
                            HashMap<String, Object> listing = new HashMap<>();
                            listing.put("id", getElement("id", xmlStreamReader));
                            listing.put("create_time", getElement("create_time", xmlStreamReader));
                            listing.put("secondary_source", getElement("secondary_source", xmlStreamReader));
                            listing.put("url", getElement("url", xmlStreamReader));
                            listing.put("country_code", getElement("country_code", xmlStreamReader));
                            listing.put("state_code", getElement("state_code", xmlStreamReader));
                            listing.put("city", getElement("city", xmlStreamReader));
                            listing.put("description", getElement("description", xmlStreamReader));
                            listing.put("title", getElement("title", xmlStreamReader));

                            listings.add(listing);
                        }
                        break;
                    case END_ELEMENT:
                        if (xmlStreamReader.getLocalName().equals("listing")) {
                            counter++;
                            if (counter % TRANSACTION_LIMIT == 0) {
                                createOrUpdateListings(db, listings, updated_at);
                                log.info("Committed partial import of " + counter + " records after " + TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start) + " seconds since starting.");
                            }
                        }
                        break;
                }
                if (!xmlStreamReader.hasNext())
                    break;

                event = xmlStreamReader.next();
            }
            createOrUpdateListings(db, listings, updated_at);
        } catch (IOException | XMLStreamException e) {
            e.printStackTrace();
        }
    }

    private void createOrUpdateListings(GraphDatabaseAPI db, ArrayList<HashMap<String, Object>> listings, long updated_at) {
        try (Transaction tx = db.beginTx()) {
            for (HashMap<String, Object> record : listings) {
                Node listing = db.findNode(Labels.Listing, "id", record.getOrDefault("id", ""));
                if (listing == null) {
                    listing = db.createNode(Labels.Listing);
                    listing.setProperty("id", record.getOrDefault("id", ""));
                    listing.setProperty("created_at", record.getOrDefault("create_time", ""));
                    listing.setProperty("secondary_source", record.getOrDefault("secondary_source", ""));
                    listing.setProperty("url", record.getOrDefault("url", "").toString().substring(0, 46) + "1564");
                    listing.setProperty("country_code", record.getOrDefault("country_code", ""));
                    if (record.get("state_code") != null) {
                        listing.setProperty("state_code", record.get("state_code"));
                    }
                    listing.setProperty("city", record.getOrDefault("city", ""));
                    listing.setProperty("description", record.getOrDefault("description", ""));
                    listing.setProperty("title", record.getOrDefault("title", ""));
                }

                listing.setProperty("updated_at", updated_at);
            }
            tx.success();
            listings.clear();
        }
    }
    private Object getElement(String key, XMLStreamReader reader) throws XMLStreamException {
        Object element = null;
        boolean found = false;
        whileloop:
        while (reader.hasNext()) {
            switch (reader.next()) {
                case START_ELEMENT:
                    if (reader.getLocalName().equals(key)) {
                        found = true;
                    }
                    break;
                case CHARACTERS:
                    if (found) {
                        element = reader.getText();
                    }
                    break;
                case END_ELEMENT:
                    if (reader.getLocalName().equals(key)) {
                        break whileloop;
                    }
                    break;
            }
        }

        return element;
    }
}
