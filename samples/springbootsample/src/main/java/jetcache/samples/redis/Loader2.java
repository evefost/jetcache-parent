package jetcache.samples.redis;


import java.util.List;

/**
 * 缓存加载回调
 * @author xieyang
 * @param <P> key 对应的入参，可作为查库的参数
 */
public interface Loader2<P> {


    /**
     * 据入参 用户自定读缓存key
     * 如果用户不实现，默认p 为String 类型的 key
     * @param param
     * @return key 缓存的key
     */
    default String  getReadKey(P param){
        if(param instanceof  String){
            return (String) param;
        }
        throw new RuntimeException("必须覆写 getReadKey 返回读缓存key");
    }


    /**
     * 没有缓存的入参回调,可以不覆写写
     * @param noCacheParams
     * @return 可以从数据查询结果集
     */
   default List<Pair<P,Object>> getFromDb(List<P> noCacheParams){
        return null;
    }


}