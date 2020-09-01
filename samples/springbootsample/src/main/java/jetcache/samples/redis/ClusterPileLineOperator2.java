package jetcache.samples.redis;

import com.alibaba.fastjson.JSON;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanWrapper;
import org.springframework.beans.BeanWrapperImpl;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;

/**
 * redis 集群管道操作工具类
 *
 * @author xieyang
 */
public class ClusterPileLineOperator2 {

    private static final Logger logger = LoggerFactory.getLogger(ClusterPileLineOperator2.class);

    private static JedisPipelineCluster cluster;

    private static ObjectMapper mapper = new ObjectMapper();

    public ClusterPileLineOperator2(JedisPipelineCluster cluster) {
        this.cluster = cluster;
    }


    public static <P> Class<?> getTargetType(Type actualValueType) {
        Class<?> targetType = null;
        if (isArrayType(actualValueType)) {
            ParameterizedType parameterizedType = (ParameterizedType) actualValueType;
            targetType = (Class<?>) parameterizedType.getActualTypeArguments()[0];
        } else {
            targetType = (Class<?>) actualValueType;
        }
        return targetType;
    }

    public static <V> List<Pair<String/*key*/, V>> batchReadPair(List<String> keys, Class<V> targetType, Loader2 Loader2, int seconds) {
        return batchReadPair(keys, targetType, Loader2, seconds, true, false, null);
    }

    /**
     * 批量查询数据,key 对应值为组数类  ，返回参值对结果(结果集无序)
     *
     * @param keys
     * @param targetType
     * @param Loader2
     * @param seconds
     * @param isArray
     * @param <P>
     * @param <?>
     * @return
     */
    public static <V> List<Pair<String/*key*/, V>> batchReadPair(List<String> keys, Class<V> targetType, Loader2 Loader2,
                                                                 int seconds, boolean isArray, boolean isHash, String hashFieldName) {

        Map<JedisPool, List<String>> poolKeys = getPoolKeys(keys);
        //缓存结果集
        List<Pair<String/*key*/, V>> cacheResultValues = new ArrayList<>(keys.size());
        List<String> hasCacheParams = new ArrayList<>();
        List<String> noCacheParams = new ArrayList<>();
        CacheResult<Pair<String, V>> cacheResult = new CacheResult();
        cacheResult.setValues(cacheResultValues);
        cacheResult.setHasValueParams(hasCacheParams);
        cacheResult.setNoValueParams(noCacheParams);
        poolKeys.keySet().stream().forEach((pool -> {
            readKeysOnPoolNode(pool, poolKeys.get(pool), targetType, isArray, cacheResult, isHash, hashFieldName);
        }));

        if (noCacheParams.isEmpty()) {
            return cacheResult.getValues();
        }
        List<Pair<String, V>> listFromDb = Loader2.getFromDb(noCacheParams);
        if (listFromDb == null || listFromDb.isEmpty()) {
            return cacheResult.getValues();
        }
        cacheResultValues.addAll(listFromDb);
        //batchWritePair(listFromDb, Loader2, seconds, isHash, hashFieldName);
        return cacheResult.getValues();
    }


    /**
     * 从某个节点中读取数据
     *
     * @param pool
     * @param keyParams
     * @param targetType
     * @param isArray
     * @param cacheResult
     * @param <P>
     * @param <?>
     */
    private static <V> void readKeysOnPoolNode(JedisPool pool, List<String> keys, Class<V> targetType,
                                               boolean isArray, CacheResult<Pair<String, V>> cacheResult, boolean isHash, String hashFieldName) {
        Jedis jedis = pool.getResource();
        Pipeline pipeline = jedis.pipelined();
        keys.forEach(key -> {
                    if (isHash) {
                        if (hashFieldName != null) {
                            pipeline.hget(key, hashFieldName);
                        } else {
                            pipeline.hgetAll(key);
                        }
                    } else {
                        pipeline.get(key);
                    }
                }
        );
        List<Object> cacheValues = pipeline.syncAndReturnAll();
        jedis.close();
        List<Pair<String, V>> cacheResultValues = cacheResult.getValues();
        List<String> hasCacheParams = cacheResult.getHasValueParams();
        List<String> noCacheParams = cacheResult.getNoValueParams();
        for (int i = 0; i < keys.size(); i++) {
            Object value = cacheValues.get(i);
            V targetValue = null;
            String key = keys.get(i);
            try {
                targetValue = parseValue(value, targetType, isArray, key);
            } catch (Exception exception) {
                logger.error("redis 缓存数据转换异常:[{}]type[{{}]", value, targetType.getName(), exception);
            }
            if (targetValue == null) {
                noCacheParams.add(key);
            } else {
                hasCacheParams.add(key);
                Pair<String, V> pair = new Pair<>(key, targetValue);
                cacheResultValues.add(pair);
            }
        }
    }

    public static Type findActualValueTypeArgument(Object targetBean) {
        Class targetClass = targetBean.getClass();
        Type[] genericInterfaces = targetClass.getGenericInterfaces();
        for (Type type : genericInterfaces) {
            if (type instanceof ParameterizedType) {
                ParameterizedType parameterizedType = (ParameterizedType) type;
                Type actualTypeArgument = parameterizedType.getActualTypeArguments()[1];
                return actualTypeArgument;
            }
        }
        return null;
    }

    public static Type findFieldActualValueTypeArgument(Class targetType, String fieldName) throws
            NoSuchFieldException {
        Field declaredField = targetType.getDeclaredField(fieldName);
        Type type = declaredField.getGenericType();
        if (type instanceof ParameterizedType) {
            ParameterizedType parameterizedType = (ParameterizedType) type;
            Type actualTypeArgument = parameterizedType.getActualTypeArguments()[0];
            return actualTypeArgument;
        }
        return null;
    }

    public static boolean isArrayType(Type type) {
        if (type instanceof ParameterizedType) {
            ParameterizedType parameterizedType = (ParameterizedType) type;
            Type rawType = parameterizedType.getRawType();
            return Collection.class.isAssignableFrom((Class<?>) rawType);
        }
        if (type instanceof Class) {
            Class cType = (Class) type;
            if (cType.isAssignableFrom(List.class)) {
                return true;
            }
        }

        return false;
    }

    /**
     * @param keys
     * @param <P>  入参实体
     * @return
     */
    public static <P> Map<JedisPool, List<String>> getPoolKeys(List<String> keys) {
        Map<JedisPool, List<String>> poolKeyMap = new HashMap<>();
        for (String key : keys) {
            JedisPool jedisPool = cluster.getPoolFromSlot(key);
            if (poolKeyMap.keySet().contains(jedisPool)) {
                List<String> poolKeys = poolKeyMap.get(jedisPool);
                poolKeys.add(key);
            } else {
                List<String> poolKeys = new ArrayList<>();
                poolKeys.add(key);
                poolKeyMap.put(jedisPool, poolKeys);
            }
        }
        return poolKeyMap;
    }


    /**
     * 批量写数据
     *
     * @param toCacheList
     * @param call
     * @param seconds     缓存有效时间
     * @param <P>
     */
    public static <P> void batchWrite(List<P> toCacheList, WriteCacheCall2<P> call, int seconds) {
        batchWrite(toCacheList, call, seconds, false);
    }


    public static <P> void batchWritePair(List<Pair<P, P>> toCacheList, WriteCacheCall2<P> call,
                                          int seconds) {
        batchWritePair(toCacheList, call, seconds, false, null);
    }


    private static <P> void batchWrite(List<P> toCacheList, WriteCacheCall2<P> call, int seconds,
                                       boolean isHash) {
        List<Pair<P, P>> collect = new ArrayList<>(toCacheList.size());
        toCacheList.forEach(v -> {
            collect.add(new Pair<P, P>(v, v));
        });
        batchWritePair(collect, call, seconds, isHash, null);
    }


    public static <P> void batchWritePair(List<Pair<P, P>> toCacheList, WriteCacheCall2<P> call,
                                          int seconds, boolean isHash, String hashFieldName) {

    }

    private static String covert2CacheValue(Object value) {
        if (value instanceof String) {
            return (String) value;
        }
        if (value instanceof Integer) {
            return value.toString();
        }
        if (value instanceof Long) {
            return value.toString();
        }
        if (value instanceof Short) {
            return value.toString();
        }
        if (value instanceof Double) {
            return value.toString();
        }
        if (value instanceof Float) {
            return value.toString();
        }
        if (value instanceof Boolean) {
            return value.toString();
        }
        try {
            return mapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            logger.error("对象转[{}]json失败:", value, e);
        }
        return null;
    }


    /**
     * 批量删除操作
     *
     * @param deleteKeys
     * @param writeCall
     * @param <?>
     */
    public static void batchDelete(List<String> deleteKeys, WriteCacheCall2 writeCall) {
        List<Pair<String, String>> queryKeyPairList = new ArrayList<>(deleteKeys.size());
        deleteKeys.forEach((key) -> queryKeyPairList.add(new Pair<>(key, key)));
        Map<JedisPool, List<String>> poolKeys = getPoolKeys(deleteKeys);
        for (JedisPool jedisPool : poolKeys.keySet()) {
            Jedis jedis = jedisPool.getResource();
            Pipeline pipeline = jedis.pipelined();
            List<String> keys = poolKeys.get(jedisPool);
            keys.forEach((key) -> pipeline.del(key));
            pipeline.sync();
            jedis.close();
        }

    }


    private static <V> V parseValue(Object srcValue, Class<V> targetType, boolean isArray, String key) throws
            NoSuchFieldException {
        if (srcValue == null) {
            return null;
        }
        if (srcValue instanceof String) {
            return (V) parseStringValue((String) srcValue, targetType, isArray);
        } else if (srcValue instanceof Map) {
            Map<String, String> map = (Map) srcValue;
            if (map.size() == 0) {
                if (logger.isDebugEnabled()) {
                    logger.debug("has key of  [{}]  value is expire ", key);
                }
                return null;
            }
            //处理hash 结果集
            BeanWrapper beanWrapper = new BeanWrapperImpl(targetType);
            for (Map.Entry<String, String> entry : map.entrySet()) {
                String hkey = entry.getKey();
                PropertyDescriptor propertyDescriptor = beanWrapper.getPropertyDescriptor(hkey);
                if (propertyDescriptor == null) {
                    continue;
                }
                Class<?> propertyType = propertyDescriptor.getPropertyType();
                if (isArrayType(propertyType)) {
                    //处理列表泛参
                    Class fieldGenericType = (Class) findFieldActualValueTypeArgument(targetType, propertyDescriptor.getName());
                    Object targetValue = parseStringValue(entry.getValue(), fieldGenericType, true);
                    beanWrapper.setPropertyValue(hkey, targetValue);
                } else {
                    Object targetValue = parseStringValue(entry.getValue(), propertyDescriptor.getPropertyType(), false);
                    beanWrapper.setPropertyValue(hkey, targetValue);
                }
            }
            return (V) beanWrapper.getWrappedInstance();
        }
        logger.error("不支持的数据转换[{}][{}]", srcValue, targetType);
        return null;
    }

    private static Object parseStringValue(String srcValue, Class<?> targetType, boolean isArray) {
        if (isArray) {
            try {
                return JSON.parseArray(srcValue, targetType);
            } catch (Exception ex) {
                logger.error("解释json[{}] 数据有问题", srcValue, ex);
            }
            return null;
        }
        if (targetType.isAssignableFrom(String.class)) {
            return srcValue;
        }
        if (targetType.isAssignableFrom(Integer.class)) {

            return Integer.parseInt(srcValue);
        }
        if (targetType.isAssignableFrom(Long.class)) {
            return Long.parseLong(srcValue);
        }
        if (targetType.isAssignableFrom(Short.class)) {
            return Short.parseShort(srcValue);
        }
        if (targetType.isAssignableFrom(Double.class)) {
            return Double.parseDouble(srcValue);
        }
        if (targetType.isAssignableFrom(Float.class)) {
            return Float.parseFloat(srcValue);
        }

        if (targetType.isAssignableFrom(Boolean.class)) {
            return Boolean.parseBoolean(srcValue);
        }
        try {
            return JSON.parseObject(srcValue, targetType);
        } catch (Exception ex) {
            logger.error("解释json[{}] targetType[{}]数据有问题", srcValue, targetType.getName(), ex);
        }
        return null;
    }
}
