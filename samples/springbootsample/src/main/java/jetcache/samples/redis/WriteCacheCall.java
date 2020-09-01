package jetcache.samples.redis;


/**
 * 写缓存key回调
 * @author xieyang
 * @param <P>
 * @param <V>
 */
public interface WriteCacheCall<P,V> {



    /**
     * 据 db 回调，用户自定义 写缓存key
     * 如果用户不实现，默认p 为String 类型的 key，其它情况必须复写
     * @param param 入参
     * @param value 值
     * @return
     */
   default String  getWriteKey(P param ,V value){
       //
       if(param instanceof  String){
           return (String) param;
       }
       throw new RuntimeException("必须复写getWriteKey 返回写缓存key");
   };
}
