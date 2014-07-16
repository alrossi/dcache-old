package diskCacheV111.poolManager;

import com.google.common.base.Preconditions;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPoolGroup;

class PGroup extends PoolCore implements SelectionPoolGroup {
    private static final long serialVersionUID = 3883973457610397314L;
    final Map<String, Pool> _poolList = new ConcurrentHashMap<>();

    private int minReplicas = 1;
    private int maxReplicas = 1;
    private boolean sameHostEnabled = true;

    PGroup(String name) {
        super(name);
    }

    @Override
    public boolean areSameHostReplicasEnabled() {
        return sameHostEnabled;
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


    public void setSameHostReplicasEnabled(boolean sameHostEnabled) {
        this.sameHostEnabled = sameHostEnabled;
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
                         + "; sameHostEnabled=" + sameHostEnabled + ")";
    }
}
