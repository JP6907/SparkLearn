import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class ZookeeperClient {

    ZooKeeper zk = null;

    @Before
    public void init() throws Exception{
        zk = new ZooKeeper("localhost:2181", 2000, null);
        //zk = new ZooKeeper("localhost:12181,localhost:22181,localhost:42181", 2000, null);
    }

    @Test
    public void testGet() throws Exception{
        // 参数1：节点路径    参数2：是否要监听    参数3：所要获取的数据的版本,null表示最新版本
        byte[] data = zk.getData("/java", false, null);
        System.out.println(new String(data,"UTF-8"));
        zk.close();
    }

    @Test
    public void testListChildren() throws Exception{
        // 参数1：节点路径    参数2：是否要监听
        // 注意：返回的结果中只有子节点名字，不带全路径
        List<String> children = zk.getChildren("/", false);
        for(String c : children){
            System.out.println(c);
        }
        zk.close();
    }


    @Test
    public void testCreate() throws Exception{
        // 参数1：要创建的节点路径  参数2：数据  参数3：访问权限  参数4：节点类型
        String create = zk.create("/java","hello java".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        //String create = zk.create("/java","hello java".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(create);
        zk.close();
    }
}
