package diskCacheV111.poolManager;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.base.Preconditions;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPoolGroup;

class PGroup extends PoolCore implements SelectionPoolGroup {
    private static final long serialVersionUID = 3883973457610397314L;
    final Map<String, Pool> _poolList = new ConcurrentHashMap<>();

    private int minReplicas = 1;
    private int maxReplicas = 1;
    private boolean sameDeviceIdsEnabled = true;

    PGroup(String name) {
        super(name);
    }

    @Override
    public boolean areSameDeviceIdsEnabled() {
        return sameDeviceIdsEnabled;
    }

    @Override
    public int getMaxReplicas() {
        return maxReplicas;
    }

    @Override
    public int getMinReplicas() {
        return minReplicas;
    }

    public void setMaxReplicas(int maxReplicas) {
        Preconditions.checkArgument(maxReplicas > 0);
        this.maxReplicas = maxReplicas;
    }

    public void setMinReplicas(int minReplicas) {
        Preconditions.checkArgument(minReplicas > 0);
        this.minReplicas = minReplicas;
    }

    private String[] getPools()
    {
        return _poolList.keySet().toArray(new String[_poolList.size()]);
    }

    public void setSameDeviceIdsEnabled(boolean sameDeviceIdsEnabled) {
        this.sameDeviceIdsEnabled = sameDeviceIdsEnabled;
    }

    public void validate() throws IllegalStateException {
        if (minReplicas > maxReplicas) {
            throw new IllegalStateException(this + ", minReplicas "
                            + "exceeds maxReplicas " + minReplicas
                            + " > " + maxReplicas);
        }
    }

    @Override
    public String toString() {
        return super.toString()
                        + "  (links=" + _linkList.size()
                        + ";pools=" + _poolList.size()
                        + "; minReplicas=" + minReplicas
                        + "; maxReplicas=" + maxReplicas
                        + "; sameDeviceIdsEnabled=" + sameDeviceIdsEnabled + ")";
    }
}
