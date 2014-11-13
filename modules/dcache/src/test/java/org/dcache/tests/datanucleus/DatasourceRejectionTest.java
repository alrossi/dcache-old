package org.dcache.tests.datanucleus;

import com.zaxxer.hikari.HikariConfig;
import org.junit.Test;

import java.util.UUID;

import org.dcache.testdata.hsqldb.HsqldbEntry;
import org.dcache.testdata.hsqldb.HsqldbStore;
import org.dcache.testdata.postgres.PostgreSQLEntry;
import org.dcache.testdata.postgres.PostgreSQLStore;
import org.dcache.testdata.xml.XmlEntry;
import org.dcache.testdata.xml.XmlStore;

import static org.junit.Assert.fail;

public class DatasourceRejectionTest {

    @Test
    public void testSimpleConnector() {
        try {
            HsqldbStore hsqldb = new HsqldbStore();
            PostgreSQLStore postgres = new PostgreSQLStore();
            XmlStore xml = new XmlStore();
            postgres.put(new PostgreSQLEntry("postgres data"));
            hsqldb.put(new HsqldbEntry("hsqld data"));
            xml.put(new XmlEntry(UUID.randomUUID().toString(),
                            "xml data"));
        } catch (Exception e) {
            fail("Unsuccessful use of xml with a datastore "
                            + "database in the same JVM: " + e.toString());
        }
    }

    @Test
    public void testHikariConnector() {
        try {
            HsqldbStore hsqldb = new HsqldbStore(new HikariConfig());
            PostgreSQLStore postgres = new PostgreSQLStore(new HikariConfig());
            XmlStore xml = new XmlStore();
            postgres.put(new PostgreSQLEntry("postgres data"));
            hsqldb.put(new HsqldbEntry("hsqld data"));
            xml.put(new XmlEntry(UUID.randomUUID().toString(),
                            "xml data"));
        } catch (Exception e) {
            fail("Unsuccessful use of xml with a datastore "
                            + "database in the same JVM: " + e.toString());
        }
    }

    @Test
    public void testAlarmsWrapperConnector() {
        try {
            HsqldbStore hsqldb = new HsqldbStore(new HikariConfig(), "hsqldb-datasource-rejection-test");
            PostgreSQLStore postgres = new PostgreSQLStore(new HikariConfig(), "postgres-datasource-rejection-test");
            XmlStore xml = new XmlStore();
            postgres.put(new PostgreSQLEntry("postgres data"));
            hsqldb.put(new HsqldbEntry("hsqld data"));
            xml.put(new XmlEntry(UUID.randomUUID().toString(),
                            "xml data"));
        } catch (Exception e) {
            fail("Unsuccessful use of xml with a datastore "
                            + "database in the same JVM: " + e.toString());
        }
    }
}
