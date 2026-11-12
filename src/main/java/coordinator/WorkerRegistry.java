package coordinator;

import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;

public class WorkerRegistry {

    private static final ConcurrentHashMap<String, Socket> workers = new ConcurrentHashMap<>();

    public static boolean register(String workerId, Socket socket) {

        return workers.putIfAbsent(workerId, socket) == null;

    }

    public static void remove(String workerId, Socket socket) {

        workers.remove(workerId, socket);
    }

    public static Socket getWorker(String workerId) {

        return workers.get(workerId);
    }

    public static int getWorkerCount() {

        return workers.size();
    }
}