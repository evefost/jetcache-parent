/**
 * Created on 2018/8/11.
 */
package jetcache.samples.springboot;

import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;

/**
 * @author <a href="mailto:areyouok@gmail.com">huangli</a>
 */
@Repository
public class UserServiceImpl implements UserService {

    @Override
    public User loadUser(long userId) {
        System.out.println("load user: " + userId);
        User user = new User();
        user.setUserId(userId);
        user.setUserName("user" + userId);
        return user;
    }

    @Override
    public User loadUser(User user, Person person) {
        System.out.println("load user: " + user.getUserId());
        User user2 = new User();
        user2.setUserId(user.getUserId());
        user2.setUserName("user" + user.getUserId());
        return user2;
    }

    @Override
    public List<User> listUser(List<User> users) {
        List<User> list = new ArrayList<>();
        User user = new User();
        user.setUserId(users.get(0).getUserId());
        User user2 = new User();
        user2.setUserId(users.get(1).getUserId());
        list.add(user);
        list.add(user2);
        return list;
    }


}
