package coordinator;

import java.net.Socket;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class WorkerRegistry {

    private static final ConcurrentHashMap<String, Socket> workers = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, WorkerHandler> handlers = new ConcurrentHashMap<>();
    private static final Map<String, Integer> workerCapacities = new ConcurrentHashMap<>();

    public static synchronized boolean register(String workerId, Socket socket) {
        return register(workerId, socket, 1);
    }

    public static synchronized boolean register(String workerId, Socket socket, int maxConcurrentChunks) {
        if (maxConcurrentChunks < 1) {
            throw new IllegalArgumentException("Worker capacity must be positive");
        }
        if (workers.putIfAbsent(workerId, socket) != null) {
            return false;
        }
        workerCapacities.put(workerId, maxConcurrentChunks);
        return true;
    }

    public static synchronized void registerHandler(String workerId, WorkerHandler handler) {
        handlers.put(workerId, handler);
    }

    public static synchronized void remove(String workerId, Socket socket) {
        if (workers.remove(workerId, socket)) {
            handlers.remove(workerId);
            workerCapacities.remove(workerId);
        }
    }

    public static Socket getWorker(String workerId) {
        return workers.get(workerId);
    }

    public static WorkerHandler getHandler(String workerId) {
        return handlers.get(workerId);
    }

    public static int getMaxConcurrentChunks(String workerId) {
        return workerCapacities.getOrDefault(workerId, 1);
    }

    public static java.util.Set<String> getWorkerIds() {
        return workers.keySet();
    }

    public static int getWorkerCount() {
        return workers.size();
    }
}