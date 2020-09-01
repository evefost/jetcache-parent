package jetcache.samples.redis;


import java.util.List;

/**
 * 缓存加载回调
 * @author xieyang
 * @param <P> key 对应的入参，可作为查库的参数
 */
public interface Loader2 {



    /**
     * 没有缓存的入参回调,可以不覆写写
     * @param noCacheKes
     * @return 可以从数据查询结果集
     */
   default <V> List<Pair<String,V>> getFromDb(List<String> noCacheKes){
        return null;
    }


}