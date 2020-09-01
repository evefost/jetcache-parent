package jetcache.samples.redis;

import java.util.List;

public class CacheResult<V> {

    /**
     * 缓存值列表
     */
    private List<V> values;

    /**
     * 有值的P
     */
    private List<String> hasValueParams;

    /**
     * 无值的P
     */
    private List<String> noValueParams;

    public List<V> getValues() {
        return values;
    }

    public void setValues(List<V> values) {
        this.values = values;
    }

    public List<String> getHasValueParams() {
        return hasValueParams;
    }

    public void setHasValueParams(List<String> hasValueParams) {
        this.hasValueParams = hasValueParams;
    }

    public List<String> getNoValueParams() {
        return noValueParams;
    }

    public void setNoValueParams(List<String> noValueParams) {
        this.noValueParams = noValueParams;
    }
}
