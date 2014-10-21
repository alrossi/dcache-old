package org.dcache.replication.data;

public final class ResilienceContraints {
    protected int minimum = 1;
    protected int maximum = 1;
    protected boolean sameHostOK = false;

    public int getMinimum() {
        return minimum;
    }

    public int getMaximum() {
        return maximum;
    }

    public boolean isSameHostOK() {
        return sameHostOK;
    }

    public void setMinimum(int minimum) {
        this.minimum = minimum;
    }

    public void setMaximum(int maximum) {
        this.maximum = maximum;
    }

    public void setSameHostOK(boolean sameHostOK) {
        this.sameHostOK = sameHostOK;
    }
}
