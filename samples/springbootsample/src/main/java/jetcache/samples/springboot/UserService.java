/**
 * Created on 2018/8/11.
 */
package jetcache.samples.springboot;

import com.alicp.jetcache.anno.CacheType;
import com.alicp.jetcache.anno.Cached;
import jetcache.samples.extend.ListCached;

import java.util.List;

/**
 * @author <a href="mailto:areyouok@gmail.com">huangli</a>
 */
public interface UserService {
    @Cached(name = "loadUser", expire = 100,cacheType = CacheType.LOCAL)
    User loadUser(long userId);

    @Cached(name = "loadUser",key = "#person.id+':'+#user.userId",expire = 100,cacheType = CacheType.LOCAL)
    User loadUser(User user,Person person);

    @ListCached(name = "loadUser",key = "#users[$].userId",expire = 100,cacheType = CacheType.LOCAL)
    List<User> listUser(List<User> users);

}
