package jetcache.samples.redis;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
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
import java.util.stream.Collectors;

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


    /**
     * 批量查询数据,key : value ，返回参值对结果(结果集无序)
     *
     * @param queryParams
     * @param loader
     * @param seconds     缓存有效时长 单位秒 ；小于0，不缓存结果
     * @param <P>         入参泛参
     * @param <V>         值泛参
     * @return
     */
    public static <P, V> List<V> batchRead(List<P> queryParams, Loader<P, V> loader, int seconds) {
        List<Pair<P, V>> pairs = batchReadPair(queryParams, loader, seconds);
        List<V> resultList = pairs.stream().map(pvPair -> pvPair.getValue()).collect(Collectors.toList());
        return resultList;
    }

    /**
     * 哈唏 批量查询数据,key : value ，返回参值对结果(结果集无序)
     *
     * @param queryParams
     * @param loader
     * @param seconds     缓存有效时长 单位秒 ；小于0，不缓存结果
     * @param <P>         入参泛参
     * @param <V>         值泛参
     * @return
     */
    public static <P, V> List<V> hbatchRead(List<P> queryParams, Loader<P, V> loader, int seconds) {
        return hbatchRead(queryParams, loader, seconds, null);
    }


    /**
     * 哈唏 批量查询数据,key : value ，返回参值对结果(结果集无序)
     *
     * @param queryParams
     * @param loader
     * @param seconds     缓存有效时长 单位秒 ；小于0，不缓存结果
     * @param <P>         入参泛参
     * @param <V>         值泛参
     * @return
     */
    public static <P, V> List<V> hbatchRead(List<P> queryParams, Loader<P, V> loader, int seconds, String hashFieldName) {
        List<Pair<P, V>> pairs = batchReadPair(queryParams, loader, seconds, true, hashFieldName);
        List<V> resultList = pairs.stream().map(pvPair -> pvPair.getValue()).collect(Collectors.toList());
        return resultList;
    }

    /**
     * 哈唏 批量查询数据,pair 入参：对应的值  ，返回参值对结果(结果集无序)
     *
     * @param queryParams
     * @param loader
     * @param seconds     缓存有效时长 单位秒 ；小于0，不缓存结果
     * @param <P>         入参泛参
     * @param <V>         值泛参
     * @return
     */
    public static <P, V> List<Pair<P, V>> hbatchReadPair(List<P> queryParams, Loader<P, V> loader, int seconds, String hashFieldName) {
        List<Pair<P, V>> pairs = batchReadPair(queryParams, loader, seconds, true, hashFieldName);
        return pairs;
    }


    /**
     * 批量查询数据,pair 入参：对应的值  ，返回参值对结果(结果集无序)
     *
     * @param queryParams
     * @param loader
     * @param seconds     缓存有效时长 单位秒 ；小于0，不缓存结果
     * @param <P>         入参泛参
     * @param <V>         值泛参
     * @return
     */
    public static <P, V> List<Pair<P, V>> batchReadPair(List<P> queryParams, Loader<P, V> loader, int seconds) {
        return batchReadPair(queryParams, loader, seconds, false, null);
    }


    private static <P, V> List<Pair<P, V>> batchReadPair(List<P> queryParams, Loader<P, V> loader, int seconds, boolean isHash, String hashFieldName) {
        Type actualValueType = findActualValueTypeArgument(loader);
        Class<V> targetType = getTargetType(actualValueType);
        CacheResult<P, Pair<P, V>> pairCacheResult = batchReadPairWithDetails(queryParams, targetType, loader, seconds, isArrayType(actualValueType), isHash, hashFieldName);
        return pairCacheResult.getValues();
    }

    public static <P, V> Class<V> getTargetType(Type actualValueType) {
        Class<V> targetType = null;
        if (isArrayType(actualValueType)) {
            ParameterizedType parameterizedType = (ParameterizedType) actualValueType;
            targetType = (Class<V>) parameterizedType.getActualTypeArguments()[0];
        } else {
            targetType = (Class<V>) actualValueType;
        }
        return targetType;
    }


    /**
     * 批量查询数据,key 对应值为组数类  ，返回参值对结果(结果集无序)
     *
     * @param queryParams
     * @param targetType
     * @param loader
     * @param seconds
     * @param isArray
     * @param <P>
     * @param <V>
     * @return
     */
    private static <P, V> CacheResult<P, Pair<P, V>> batchReadPairWithDetails(List<P> queryParams, Class<V> targetType, Loader<P, V> loader, int seconds, boolean isArray, boolean isHash, String hashFieldName) {
        List<Pair<String/*key*/, P>> pairQueryKeys = new ArrayList<>(queryParams.size());
        queryParams.forEach(p -> {
            pairQueryKeys.add(new Pair<>(loader.getReadKey(p), p));
        });
        Map<JedisPool, List<Pair<String, P>>> poolKeys = getPoolKeys(pairQueryKeys);
        //缓存结果集
        List<Pair<P, V>> cacheResultValues = new ArrayList<>(pairQueryKeys.size());
        List<P> hasCacheParams = new ArrayList<>();
        List<P> noCacheParams = new ArrayList<>();
        CacheResult<P, Pair<P, V>> cacheResult = new CacheResult();
        cacheResult.setValues(cacheResultValues);
        cacheResult.setHasValueParams(hasCacheParams);
        cacheResult.setNoValueParams(noCacheParams);
        poolKeys.keySet().stream().forEach((pool -> {
            readKeysOnPoolNode(pool, poolKeys.get(pool), targetType, isArray, cacheResult, isHash, hashFieldName);
        }));

        if (noCacheParams.isEmpty()) {
            return cacheResult;
        }
        List<Pair<P, V>> listFromDb = loader.getFromDb(noCacheParams);
        if (listFromDb == null || listFromDb.isEmpty()) {
            return cacheResult;
        }
        cacheResultValues.addAll(listFromDb);
        batchWritePair(listFromDb, loader, seconds, isHash, hashFieldName);
        return cacheResult;
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
     * @param <V>
     */
    private static <P, V> void readKeysOnPoolNode(JedisPool pool, List<Pair<String, P>> keyParams, Class<V> targetType, boolean isArray, CacheResult<P, Pair<P, V>> cacheResult, boolean isHash, String hashFieldName) {
        Jedis jedis = pool.getResource();
        Pipeline pipeline = jedis.pipelined();
        keyParams.forEach(keyParam -> {
                    if (isHash) {
                        if (hashFieldName != null) {
                            pipeline.hget(keyParam.getKey(), hashFieldName);
                        } else {
                            pipeline.hgetAll(keyParam.getKey());
                        }
                    } else {
                        pipeline.get(keyParam.getKey());
                    }
                }
        );
        List<Object> cacheValues = pipeline.syncAndReturnAll();
        jedis.close();
        List<Pair<P, V>> cacheResultValues = cacheResult.getValues();
        List<P> hasCacheParams = cacheResult.getHasValueParams();
        List<P> noCacheParams = cacheResult.getNoValueParams();
        for (int i = 0; i < keyParams.size(); i++) {
            Pair<String, P> keyParam = keyParams.get(i);
            Object value = cacheValues.get(i);
            V targetValue = null;
            try {
                targetValue = (V) parseValue(value, targetType, isArray, keyParam);
            } catch (Exception exception) {
                logger.error("redis 缓存数据转换异常:[{}]type[{{}]", value, targetType.getName(), exception);
            }

            P param = keyParam.getValue();
            if (targetValue == null) {
                noCacheParams.add(param);
            } else {
                hasCacheParams.add(param);
                Pair<P, V> pair = new Pair<>(param, targetValue);
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

    public static Type findFieldActualValueTypeArgument(Class targetType, String fieldName) throws NoSuchFieldException {
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
     * @param queryKeyPairList
     * @param <P>              入参实体
     * @return
     */
    public static <P> Map<JedisPool, List<Pair<String, P>>> getPoolKeys(List<Pair<String, P>> queryKeyPairList) {
        Map<JedisPool, List<Pair<String, P>>> poolKeyMap = new HashMap<>();
        for (Pair<String, P> pair : queryKeyPairList) {
            String key = pair.getKey();
            JedisPool jedisPool = cluster.getPoolFromSlot(key);
            if (poolKeyMap.keySet().contains(jedisPool)) {
                List<Pair<String, P>> poolKeys = poolKeyMap.get(jedisPool);
                poolKeys.add(pair);
            } else {
                List<Pair<String, P>> poolKeys = new ArrayList<>();
                poolKeys.add(pair);
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
     * @param <V>
     */
    public static <V> void batchWrite(List<V> toCacheList, WriteCacheCall<V, V> call, int seconds) {
        batchWrite(toCacheList, call, seconds, false);
    }


    public static <P, V> void batchWritePair(List<Pair<P, V>> toCacheList, WriteCacheCall<P, V> call, int seconds) {
        batchWritePair(toCacheList, call, seconds, false, null);
    }


    public static <V> void hbatchWrite(List<V> toCacheList, WriteCacheCall<V, V> call, int seconds) {
        List<Pair<V, V>> collect = new ArrayList<>(toCacheList.size());
        toCacheList.forEach(v -> {
            collect.add(new Pair<V, V>(v, v));
        });
        batchWritePair(collect, call, seconds, true, null);
    }

    public static <V> void hbatchWrite(List<V> toCacheList, WriteCacheCall<V, V> call, int seconds, String hashFieldName) {
        List<Pair<V, V>> collect = new ArrayList<>(toCacheList.size());
        toCacheList.forEach(v -> {
            collect.add(new Pair<V, V>(v, v));
        });
        batchWritePair(collect, call, seconds, true, hashFieldName);
    }

    private static <V> void batchWrite(List<V> toCacheList, WriteCacheCall<V, V> call, int seconds, boolean isHash) {
        List<Pair<V, V>> collect = new ArrayList<>(toCacheList.size());
        toCacheList.forEach(v -> {
            collect.add(new Pair<V, V>(v, v));
        });
        batchWritePair(collect, call, seconds, isHash, null);
    }


    public static <P, V> void batchWritePair(List<Pair<P, V>> toCacheList, WriteCacheCall<P, V> call, int seconds, boolean isHash, String hashFieldName) {
        if (seconds <= 0) {
            return;
        }
        //转换缓存key
        List<Pair<String/*cacheKey*/, V>> queryKeyPairList = new ArrayList<>(toCacheList.size());
        toCacheList.forEach(pv -> {
            Pair<String, V> keyValue = new Pair<>(call.getWriteKey(pv.getKey(), pv.getValue()), pv.getValue());
            queryKeyPairList.add(keyValue);
        });

        Map<JedisPool, List<Pair<String, V>>> writePoolKeys = getPoolKeys(queryKeyPairList);
        for (JedisPool jedisPool : writePoolKeys.keySet()) {
            Jedis jedis = jedisPool.getResource();
            Pipeline pipeline = jedis.pipelined();
            List<Pair<String, V>> poolData = writePoolKeys.get(jedisPool);
            poolData.forEach(pair -> {
                V value = pair.getValue();
                String jsonValue = covert2CacheValue(value);
                if (isHash) {
                    if (hashFieldName != null) {
                        pipeline.hset(pair.getKey(), hashFieldName, jsonValue);

                    } else {
                        TypeReference<Map<String, String>> typeReference = new TypeReference<Map<String, String>>() {
                        };
                        Map<String, String> hashMap = JSON.parseObject(jsonValue, typeReference);
                        pipeline.hmset(pair.getKey(), hashMap);
                    }
                    pipeline.expire(pair.getKey(), seconds);
                } else {
                    pipeline.setex(pair.getKey(), seconds, jsonValue);
                }

            });
            pipeline.sync();
            jedis.close();
        }
    }

    private static <V> String covert2CacheValue(V value) {
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
     * @param deleteParams
     * @param writeCall
     * @param <V>
     */
    public static <V> void batchDelete(List<V> deleteParams, WriteCacheCall<V, V> writeCall) {
        List<String> deleteKeys = deleteParams.stream().map(p -> writeCall.getWriteKey(p, p)).collect(Collectors.toList());
        List<Pair<String, String>> queryKeyPairList = new ArrayList<>(deleteKeys.size());
        deleteKeys.forEach((key) -> queryKeyPairList.add(new Pair<>(key, key)));
        Map<JedisPool, List<Pair<String, String>>> poolKeys = getPoolKeys(queryKeyPairList);
        for (JedisPool jedisPool : poolKeys.keySet()) {
            Jedis jedis = jedisPool.getResource();
            Pipeline pipeline = jedis.pipelined();
            List<Pair<String, String>> keyPairs = poolKeys.get(jedisPool);
            keyPairs.forEach((pair) -> pipeline.del(pair.getKey()));
            pipeline.sync();
            jedis.close();
        }

    }


    private static <P, V> Object parseValue(Object srcValue, Class<V> targetType, boolean isArray, Pair<String, P> keyParam) throws NoSuchFieldException {
        if (srcValue == null) {
            return null;
        }
        if (srcValue instanceof String) {
            return parseStringValue((String) srcValue, targetType, isArray);
        } else if (srcValue instanceof Map) {
            Map<String, String> map = (Map) srcValue;
            if (map.size() == 0) {
                if (logger.isDebugEnabled()) {
                    logger.debug("has key of  [{}]  value is expire ", keyParam.getKey());
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
                    Object targetValue  =parseStringValue(entry.getValue(), fieldGenericType, true);
                    beanWrapper.setPropertyValue(hkey, targetValue);
                } else {
                    Object targetValue = parseStringValue(entry.getValue(), propertyDescriptor.getPropertyType(),false);
                    beanWrapper.setPropertyValue(hkey, targetValue);
                }
            }
            return beanWrapper.getWrappedInstance();
        }
        logger.error("不支持的数据转换[{}][{}]", srcValue, targetType);
        return null;
    }

    private static <V> Object parseStringValue(String srcValue, Class<V> targetType, boolean isArray) {
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
