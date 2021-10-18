import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

public class NodeManager {
    static class WorkStatus {
        private final String host, port;
        private TTransport transport;
        private BcryptService.Client client;
        private boolean busy;
        private int loads;
        private long lastTime;
    }

    WorkStatus(String host, String port, long time) {
        this.host = host;
        this.port = port;
        this.lastTime = time;
        transport = new TFramedTransport(new TSocket(host, Integer.parseInt(port)));
        client = new BcryptService.Client(new TBinaryProtocol(transport));
    }
    TTransport getTransport() {
        return transport;
    }

    BcryptService.Client getClient() {
        return client;
    }

    boolean isBusy() {
        return busy;
    }

    int getLoad() {
        return loads;
    }
    void addLoad(int password_load, short logRounds) {
        loads += password_load * Math.pow(2,logRounds);
    } 

    void reduceLoad(int password_load, short logRounds) {
        loads -= password_load * Math.pow(2,logRounds);
    } 

    
    void NodeIsBusy(boolean busy) {
        this.busy = busy;
    }

    // check if BE node is alive
    boolean BEIsAlive() {
        return System.currentTimeMillis() - lastTime < Duration.ofSeconds(5).toMillis();
    }
   

    public static Map<String, WorkStatus> workers = new ConcurrentHashMap<>();

    static checkAvaliable () {
        if (workers.isEmpty()) {
            return null;
        } else {
            findWorker(host, port);
            addWorker(host, port);
        }
    }
    static findWorker() {
        WorkStatus worker;
        WorkStatus qualified = workers.values().filter(!worker.isBusy() && worker.BEIsAlive()).findFirst();
        if (qualified != null) {
            qualified.NodeIsBusy(true);
            return qualified;
        } else {
            WorkStatus allBusy = workers.values().min(Comparator.comparingInt(WorkStatus::getLoad));
            if (allBusy != null) {
                transport = new TFramedTransport(new TSocket(host, Integer.parseInt(port)));
                client = new BcryptService.Client(new TBinaryProtocol(transport));
            } else {
                System.println ("No Avaliable Nodes Founded");
            }
        }

    }

    static void addWorker(String host, String port) {
        final String key = host + port;
        final long currentTime = System.currentTimeMillis();
        if (workers.containsKey(key)) {
            workers.get(key).lastTime = currentTime;
        } else {
            workers.put(key, new WorkStatus(host, port, currentTime));         
        }
        System.out.printf("Current worker pool size = (%s):\n", workers.size());
    }
    static void removeWorker(WorkStatus worker) {
        final String key = worker.host + worker.port;
        workers.remove(key);
    }

   

} 