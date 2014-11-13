package org.dcache.testdata.xml;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;

import org.dcache.testdata.Store;

public class XmlStore extends Store {
    private static final String EMPTY_XML_STORE
        = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<entries></entries>\n";

    public XmlStore() throws IOException {
        super(getURL());
    }

    private static String getURL() throws IOException {
        File xml = new File("./store.xml");
        if (!xml.exists()) {
            if (!xml.getParentFile().isDirectory()) {
                String parent = xml.getParentFile().getAbsolutePath();
                throw new FileNotFoundException(parent + " is not a directory");
            }

            try (FileWriter fw = new FileWriter(xml, false)) {
                fw.write(EMPTY_XML_STORE);
                fw.flush();
            }
        }

        return "xml:file:" + xml.getAbsolutePath();
    }

    @Override
    protected void setOptionalProperties(Properties properties) {
        properties.setProperty("javax.jdo.option.DetachAllOnCommit", "false");
        properties.setProperty("javax.jdo.option.Optimistic", "true");
        properties.setProperty("javax.jdo.option.NontransactionalRead", "false");
        properties.setProperty("javax.jdo.option.RetainValues", "true");
        properties.setProperty("javax.jdo.option.Multithreaded", "true");
        properties.setProperty("datanucleus.autoCreateSchema", "false");
        properties.setProperty("datanucleus.validateTables", "false");
        properties.setProperty("datanucleus.validateConstraints", "false");
        properties.setProperty("datanucleus.autoCreateColumns", "false");
        properties.setProperty("datanucleus.connectionPoolingType", "None");
        properties.setProperty("datanucleus.rdbms.CheckExistTablesOrViews", "false");
        properties.setProperty("datanucleus.rdbms.initializeColumnInfo", "None");
        properties.setProperty("datanucleus.identifier.case", "LowerCase");
        properties.setProperty("datanucleus.autoStartMechanism", "false");
        properties.setProperty("datanucleus.manageRelationships", "false");
        properties.setProperty("datanucleus.rdbms.statementBatchLimit", "1");
        properties.setProperty("datanucleus.persistenceByReachabilityAtCommit", "false");
        properties.setProperty("datanucleus.rdbms.stringLengthExceededAction", "TRUNCATE");
        properties.setProperty("datanucleus.query.jdoql.allowAll", "true");
    }
}
