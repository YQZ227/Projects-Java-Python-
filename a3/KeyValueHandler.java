import java.util.*;
import java.util.concurrent.*;

import org.apache.thrift.*;
import org.apache.thrift.server.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.*;
import org.apache.curator.*;
import org.apache.curator.retry.*;
import org.apache.curator.framework.*;
import org.apache.curator.framework.api.*;
import org.apache.log4j.*;
import org.apache.curator.utils.*;

import java.util.concurrent.locks.Lock;  
import java.util.concurrent.locks.ReentrantLock;


public class KeyValueHandler implements KeyValueService.Iface, CuratorWatcher {
    static Logger log;

    private Map<String, String> myMap;
    private CuratorFramework curClient;
    private String zkNode;
    private String host;
    private int port;
    private boolean is_primary = true;
    private KeyValueService.Client backup_client = null;
    private Lock write_lock = new ReentrantLock();

    public KeyValueHandler(String host, int port, CuratorFramework curClient, String zkNode) throws Exception {
        this.host = host;
        this.port = port;
        this.curClient = curClient;
        this.zkNode = zkNode;
        myMap = new ConcurrentHashMap<String, String>();
        log = Logger.getLogger(KeyValueHandler.class.getName());
        
        curClient.sync();
        List<String> children = curClient.getChildren().usingWatcher(this).forPath(zkNode);
    }

    public String get(String key) throws org.apache.thrift.TException
    {	
        String ret = myMap.get(key);
        if (ret == null)
            return "";
        else
            return ret;
    }

    public void put(String key, String value) throws org.apache.thrift.TException
    {   write_lock.lock();
	    myMap.put(key, value);

        if(this.backup_client != null) {
            this.backup_client.put_backup(key,value);
        }

        write_lock.unlock();
        //TODO: add put_backup to the backup node
    }

    //Primary will call this to backup during put operation.
    public void put_backup(String key, String value) throws org.apache.thrift.TException
    {
        myMap.put(key, value);
    }

    public void copy_to_backup(Map<String, String> data) throws org.apache.thrift.TException {
        log.info("Receiving backup data");
        myMap = new ConcurrentHashMap<String, String>(data);
    }

    //this function will be called when the children of zkNode changed, i.e., a child is added or removed.
    //when a handler crashes, its corresponding child will be deleted and this function will be called.
    synchronized public void process(WatchedEvent event) throws Exception {
        log.info("ZooKeeper event " + event);
        curClient.sync();
        List<String> children = curClient.getChildren().usingWatcher(this).forPath(zkNode);
    
        //determine
        Collections.sort(children);
        byte[] data = curClient.getData().forPath(zkNode + "/" + children.get(0));
        String strData = new String(data);
        log.info("primarydata:"+strData);
        String[] primary = strData.split(":");
        String primary_host = primary[0];
        int primary_port = Integer.parseInt(primary[1]);
        
        if(host.equals(primary_host) && port == primary_port){
            this.is_primary = true;
            log.info("Set to primary");
        } else {
            this.is_primary = false;
            log.info("Set to backup");
        }

        //if we are primary, update the backup record
        if(is_primary) {
            if(children.size() == 1) {
                this.backup_client = null;
            } else {
                log.info("Copy data to backup");
                byte[] backup_data = curClient.getData().forPath(zkNode + "/" + children.get(1));
                String backup_strData = new String(backup_data);
                log.info("backup data:"+backup_strData);
                String[] backup = backup_strData.split(":");
                String backup_host = backup[0];
                int backup_port = Integer.parseInt(backup[1]);

                TSocket sock = new TSocket(backup_host, backup_port);
		        TTransport transport = new TFramedTransport(sock);
		        transport.open();
		        TProtocol protocol = new TBinaryProtocol(transport);
		        this.backup_client = new KeyValueService.Client(protocol);

                //prevent write operations while copying data
                write_lock.lock();
                this.backup_client.copy_to_backup(this.myMap);
                write_lock.unlock();
            }
        }
    }
}
