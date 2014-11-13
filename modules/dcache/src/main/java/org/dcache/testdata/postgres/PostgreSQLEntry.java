package org.dcache.testdata.postgres;

public class PostgreSQLEntry {
    private String info;

    public PostgreSQLEntry(String info) {
        this.info = info;
    }

    public String getInfo() {
        return info;
    }

    public void setInfo(String info) {
        this.info = info;
    }
}
