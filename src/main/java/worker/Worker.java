package worker;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import protocol.Message;
import protocol.Message.JsonObject;

public final class Worker implements AutoCloseable {

    private static final long HEARTBEAT_INTERVAL_MS = 2000;
    private static final long TASK_RETRY_DELAY_MS = 1000;

    private final String host;
    private final int port;
    private final String requestedWorkerId;
    private final int maxConcurrentChunks;
    private final Map<String, Boolean> activeTasks = new ConcurrentHashMap<>();
    private final ExecutorService taskExecutor;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    private final Object writeLock = new Object();
    private final Object lifecycleLock = new Object();

    private volatile String workerId;
    private volatile Socket socket;
    private volatile BufferedReader input;
    private volatile PrintWriter output;
    private volatile boolean running;
    private boolean closed;

    public Worker(String host, int port, String workerId, int maxConcurrentChunks) {
        if (host == null || host.trim().isEmpty()) {
            throw new IllegalArgumentException("Coordinator host is required");
        }
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("Coordinator port must be between 1 and 65535");
        }
        if (workerId == null || workerId.trim().isEmpty()) {
            throw new IllegalArgumentException("Worker ID is required");
        }
        if (maxConcurrentChunks < 1) {
            throw new IllegalArgumentException("At least one execution slot is required");
        }

        this.host = host;
        this.port = port;
        this.requestedWorkerId = workerId;
        this.workerId = workerId;
        this.maxConcurrentChunks = maxConcurrentChunks;
        this.taskExecutor = Executors.newFixedThreadPool(maxConcurrentChunks);
    }

    public void run() throws IOException {
        synchronized (lifecycleLock) {
            if (closed) {
                return;
            }
        }
        try {
            connect();
            synchronized (lifecycleLock) {
                if (closed) {
                    return;
                }
                running = true;
                startHeartbeats();
            }
            requestTaskIfSlotAvailable();
            listen();
        } finally {
            close();
        }
    }

    private void connect() throws IOException {
        Socket connectedSocket = new Socket();
        try {
            connectedSocket.connect(new InetSocketAddress(host, port), 5000);
            connectedSocket.setTcpNoDelay(true);
        } catch (IOException exception) {
            try {
                connectedSocket.close();
            } catch (IOException closeException) {
                exception.addSuppressed(closeException);
            }
            throw exception;
        }

        BufferedReader connectedInput = new BufferedReader(
                new InputStreamReader(connectedSocket.getInputStream(), StandardCharsets.UTF_8));
        PrintWriter connectedOutput = new PrintWriter(connectedSocket.getOutputStream(), true, StandardCharsets.UTF_8);

        synchronized (lifecycleLock) {
            if (closed) {
                connectedSocket.close();
                throw new IOException("Worker was stopped before the coordinator connection was established");
            }
            socket = connectedSocket;
            input = connectedInput;
            output = connectedOutput;
        }

        System.out.println("Connecting to coordinator " + host + ":" + port + " as " + requestedWorkerId);
        send(Message.registerWorker(requestedWorkerId, maxConcurrentChunks));

        connectedSocket.setSoTimeout(5000);
        String responseLine;
        try {
            responseLine = connectedInput.readLine();
        } catch (IOException exception) {
            connectedSocket.close();
            throw new IOException("Timed out waiting for coordinator registration response", exception);
        }
        connectedSocket.setSoTimeout(0);

        if (responseLine == null) {
            connectedSocket.close();
            throw new IOException("Coordinator closed the connection during registration");
        }

        Message response = parseMessage(responseLine);
        if (!"REGISTER_SUCCESS".equals(response.type)) {
            connectedSocket.close();
            throw new IOException("Unexpected registration response: " + response.type);
        }
        if (response.payload != null) {
            Object assignedIdValue = response.payload.get("workerId");
            if (assignedIdValue instanceof String) {
                String assignedId = (String) assignedIdValue;
                if (!assignedId.trim().isEmpty()) {
                    workerId = assignedId;
                }
            }
        }

        System.out.println("Worker " + workerId + " connected and registered");
    }

    private void startHeartbeats() {
        scheduler.scheduleAtFixedRate(() -> {
            if (running) {
                try {
                    send(Message.heartbeat(workerId, activeTasks.size()));
                    System.out.println("Heartbeat sent (active tasks: " + activeTasks.size()
                            + ", available slots: " + availableSlots() + ")");
                } catch (IOException exception) {
                    System.err.println("Unable to send heartbeat: " + exception.getMessage());
                    close();
                }
            }
        }, HEARTBEAT_INTERVAL_MS, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    private void listen() throws IOException {
        try {
            String line;
            while (running && (line = input.readLine()) != null) {
                Message message;
                try {
                    message = parseMessage(line);
                } catch (IllegalArgumentException | IllegalStateException exception) {
                    System.err.println("Ignoring invalid coordinator message: " + exception.getMessage());
                    continue;
                }

                if (message == null || message.type == null || message.payload == null) {
                    System.err.println("Ignoring coordinator message with missing type or payload");
                    continue;
                }

                switch (message.type) {
                    case Message.TASK_ASSIGN:
                        acceptTask(message.payload);
                        break;
                    case Message.NO_TASK_AVAILABLE:
                        scheduler.schedule(this::requestTaskIfSlotAvailable,
                                TASK_RETRY_DELAY_MS, TimeUnit.MILLISECONDS);
                        break;
                    default:
                        System.err.println("Ignoring unsupported coordinator message: " + message.type);
                        break;
                }
            }
        } catch (IOException exception) {
            if (running) {
                throw exception;
            }
        }

        if (running) {
            System.out.println("Coordinator closed the connection");
        }
    }

    private void acceptTask(JsonObject payload) {
        Object taskIdValue = payload.get("taskId");
        if (!(taskIdValue instanceof String)) {
            System.err.println("Ignoring TASK_ASSIGN without taskId");
            return;
        }
        String taskId = (String) taskIdValue;
        if (taskId.trim().isEmpty()) {
            System.err.println("Ignoring TASK_ASSIGN with empty taskId");
            return;
        }
        if (activeTasks.size() >= maxConcurrentChunks) {
            System.err.println("Cannot accept task " + taskId + ": all execution slots are busy");
            return;
        }
        if (activeTasks.putIfAbsent(taskId, Boolean.TRUE) != null) {
            System.err.println("Ignoring duplicate active task " + taskId);
            return;
        }

        System.out.println("Accepted task " + taskId);
        System.out.println("Task input: " + payload);

        try {
            taskExecutor.submit(() -> executeTask(taskId, payload));
            requestTaskIfSlotAvailable();
        } catch (RuntimeException exception) {
            activeTasks.remove(taskId);
            System.err.println("Could not schedule task " + taskId + ": " + exception.getMessage());
        }
    }

    private void executeTask(String taskId, JsonObject payload) {
        boolean success = false;
        List<List<BigDecimal>> result = null;
        String errorMessage = null;
        try {
            List<?> matrixA = requireArray(payload, "matrixA");
            List<?> matrixB = requireArray(payload, "matrixB");
            List<?> rowRange = requireArray(payload, "rowRange");

            System.out.println("Executing matrix multiplication for task " + taskId);
            result = ComputeEngine.multiply(matrixA, matrixB, rowRange);
            success = true;
            System.out.println("Task " + taskId + " completed: " + result);
        } catch (IllegalArgumentException | IllegalStateException exception) {
            errorMessage = exception.getMessage();
            System.err.println("Task " + taskId + " failed: " + errorMessage);
        } finally {
            activeTasks.remove(taskId);
        }

        JsonObject resultPayload = new JsonObject();
        resultPayload.addProperty("taskId", taskId);
        resultPayload.addProperty("success", success);
        resultPayload.add("result", result);
        resultPayload.add("errorMessage", errorMessage);

        try {
            send(new Message(Message.TASK_RESULT, workerId, resultPayload));
            requestTaskIfSlotAvailable();
        } catch (IOException exception) {
            System.err.println("Unable to report task " + taskId + ": " + exception.getMessage());
            close();
        }
    }

    private static List<?> requireArray(JsonObject payload, String field) {
        Object value = payload.get(field);
        if (!(value instanceof List<?>)) {
            throw new IllegalArgumentException("Task is missing array field " + field);
        }
        return (List<?>) value;
    }

    private void requestTaskIfSlotAvailable() {
        if (running && availableSlots() > 0) {
            try {
                send(Message.taskRequest(workerId));
            } catch (IOException exception) {
                System.err.println("Unable to request task: " + exception.getMessage());
                close();
            }
        }
    }

    private int availableSlots() {
        return Math.max(0, maxConcurrentChunks - activeTasks.size());
    }

    private void send(Message message) throws IOException {
        synchronized (writeLock) {
            if (!running && !Message.REGISTER.equals(message.type)) {
                throw new IOException("Worker is not connected");
            }
            if (output == null) {
                throw new IOException("Coordinator connection is not available");
            }
            output.print(message.toWireString());
            output.flush();
            if (output.checkError()) {
                throw new IOException("Failed to write message to coordinator");
            }
        }
    }

    private static Message parseMessage(String line) {
        Message message = Message.fromWireString(line);
        if (message == null) {
            throw new IllegalArgumentException("Message must be a valid JSON object");
        }
        return message;
    }

    @Override
    public void close() {
        Socket connectedSocket;
        synchronized (lifecycleLock) {
            if (closed) {
                return;
            }
            closed = true;
            running = false;
            connectedSocket = socket;
            socket = null;
        }
        scheduler.shutdownNow();
        taskExecutor.shutdownNow();
        if (connectedSocket != null) {
            try {
                connectedSocket.close();
            } catch (IOException exception) {
                System.err.println("Error closing coordinator connection: " + exception.getMessage());
            }
        }
        System.out.println("Worker " + workerId + " stopped");
    }
}
