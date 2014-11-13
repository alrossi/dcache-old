package org.dcache.testdata.xml;

public class XmlEntry {
    private String key;
    private String info;

    public XmlEntry(String key, String info) {
        this.key = key;
        this.info = info;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getInfo() {
        return info;
    }

    public void setInfo(String info) {
        this.info = info;
    }
}
