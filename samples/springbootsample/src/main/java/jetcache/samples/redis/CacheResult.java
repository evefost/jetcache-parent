package jetcache.samples.redis;

import java.util.List;

public class CacheResult<P,V> {

    /**
     * 缓存值列表
     */
    private List<V> values;

    /**
     * 有值的P
     */
    private List<P> hasValueParams;

    /**
     * 无值的P
     */
    private List<P> noValueParams;

    public List<V> getValues() {
        return values;
    }

    public void setValues(List<V> values) {
        this.values = values;
    }

    public List<P> getHasValueParams() {
        return hasValueParams;
    }

    public void setHasValueParams(List<P> hasValueParams) {
        this.hasValueParams = hasValueParams;
    }

    public List<P> getNoValueParams() {
        return noValueParams;
    }

    public void setNoValueParams(List<P> noValueParams) {
        this.noValueParams = noValueParams;
    }
}
