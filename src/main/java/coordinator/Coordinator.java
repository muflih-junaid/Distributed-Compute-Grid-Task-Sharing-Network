package coordinator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import java.io.PrintWriter;

import protocol.Message;
import protocol.Message.JsonObject;

public class Coordinator implements Runnable {

    private final int port;
    private final TaskQueue taskQueue = new TaskQueue();
    private final Map<String, TaskQueue.TaskAssignment> inFlightAssignments = new ConcurrentHashMap<>();
    private final Map<String, Set<String>> workerAssignments = new ConcurrentHashMap<>();
    private final Map<String, List<TaskQueue.TaskAssignment>> jobTaskAssignments = new ConcurrentHashMap<>();
    private final Map<String, Map<String, List<List<?>>>> jobResults = new ConcurrentHashMap<>();
    private final Set<String> completedJobs = ConcurrentHashMap.newKeySet();

    public Coordinator(int port) {
        this.port = port;
    }

    @Override
    public void run() {
        startHeartbeatMonitor();
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("Coordinator listening on " + port);

            while (true) {
                Socket socket = serverSocket.accept();
                new Thread(() -> handleConnection(socket)).start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void handleConnection(Socket socket) {
        try {
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            String line = in.readLine();

            if (line == null) {
                socket.close();
                return;
            }

            Message message = Message.fromWireString(line);
            if (message == null || message.type == null) {
                System.err.println("Invalid message received; closing connection");
                socket.close();
                return;
            }

            switch (message.type) {
                case Message.REGISTER:
                    WorkerHandler workerHandler = new WorkerHandler(socket, in, message, this);
                    workerHandler.handle();
                    break;

                case Message.JOB_SUBMIT:
                    handleJobSubmission(message, socket);
                    break;

                default:
                    System.out.println("Unknown message: " + message.type);
                    socket.close();
                    break;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void dispatchNextTaskTo(String workerId) {
        dispatchNextTaskTo(workerId, true);
    }

    private boolean dispatchNextTaskTo(
            String workerId,
            boolean notifyWhenUnavailable) {

        WorkerHandler workerHandler = WorkerRegistry.getHandler(workerId);

        if (workerHandler == null) {
            return false;
        }

        Set<String> assignedTasks = workerAssignments.computeIfAbsent(
                workerId,
                ignored -> ConcurrentHashMap.newKeySet());

        synchronized (assignedTasks) {

            int capacity = WorkerRegistry.getMaxConcurrentChunks(workerId);

            if (assignedTasks.size() >= capacity) {
                if (notifyWhenUnavailable) {
                    workerHandler.sendMessage(Message.noTaskAvailable(workerId));
                }
                return false;
            }

            TaskQueue.TaskAssignment task = taskQueue.pollNextTask();

            if (task == null) {
                if (notifyWhenUnavailable) {
                    workerHandler.sendMessage(Message.noTaskAvailable(workerId));
                }
                return false;
            }

            // Try sending the task before recording it as in-flight.
            boolean sent = workerHandler.sendMessage(
                    task.toTaskAssignMessage(workerId));

            if (!sent) {
                System.err.println(
                        "Failed to send task " + task.getTaskId()
                                + " to Worker " + workerId);

                // Close the failed Worker connection.
                workerHandler.disconnect();

                // Requeue the original task.
                retryAndRequeueTask(
                        task,
                        "Task transmission failed");

                return false;
            }

            // Record the assignment only after successful transmission.
            assignedTasks.add(task.getTaskId());
            inFlightAssignments.put(task.getTaskId(), task);

            System.out.println(
                    "Assigned " + task.getTaskId()
                            + " to worker " + workerId);

            return true;
        }
    }

    public void dispatchAvailableTasks() {
        boolean dispatched;
        do {
            dispatched = false;
            for (String workerId : WorkerRegistry.getWorkerIds()) {
                if (dispatchNextTaskTo(workerId, false)) {
                    dispatched = true;
                }
            }
        } while (dispatched && taskQueue.hasPendingTasks());
    }

    public void handleTaskResult(String workerId, JsonObject payload) {
        if (payload == null) {
            return;
        }

        Object taskIdValue = payload.get("taskId");
        if (!(taskIdValue instanceof String)) {
            System.err.println("Worker " + workerId + " reported a task result without a taskId");
            return;
        }

        String taskId = (String) taskIdValue;
        Set<String> assignedTasks = workerAssignments.get(workerId);
        if (assignedTasks == null) {
            System.err.println("Ignoring task completion for unknown or already completed task " + taskId);
            return;
        }

        TaskQueue.TaskAssignment task;
        synchronized (assignedTasks) {
            if (!assignedTasks.remove(taskId)) {
                System.err.println("Ignoring task completion for unknown or already completed task " + taskId);
                return;
            }
            task = inFlightAssignments.remove(taskId);
        }
        if (task == null) {
            System.err.println("Ignoring task completion for unknown or already completed task " + taskId);
            return;
        }

        Object successValue = payload.get("success");
        if (!Boolean.TRUE.equals(successValue)) {

            String errorMessage = String.valueOf(
                    payload.get("errorMessage"));

            System.err.println(
                    "Task " + taskId + " failed on worker "
                            + workerId + ": " + errorMessage);

            retryAndRequeueTask(task, errorMessage);

            return;
        }

        Object resultValue = payload.get("result");

        if (!(resultValue instanceof List<?>)) {

            System.err.println(
                    "Task " + taskId
                            + " completed without valid result rows.");

            retryAndRequeueTask(
                    task,
                    "Worker returned an invalid result");

            return;
        }

        String jobId = task.getJobId();
        Map<String, List<List<?>>> partialResults = jobResults.computeIfAbsent(jobId,
                ignored -> new ConcurrentHashMap<>());
        partialResults.put(taskId, asMatrixRows((List<?>) resultValue));
        taskQueue.markTaskCompleted(taskId);

        List<TaskQueue.TaskAssignment> tasksForJob = jobTaskAssignments.get(jobId);

        if (tasksForJob != null &&
                partialResults.size() == tasksForJob.size() &&
                completedJobs.add(jobId)) {

            // Assemble all Worker results into the final matrix
            List<List<?>> finalMatrix = assembleJobResult(tasksForJob, partialResults);

            System.out.println("Job " + jobId + " complete: " + finalMatrix);

            // Calculate total elapsed time
            long startTime = jobStartTimes.getOrDefault(
                    jobId, System.currentTimeMillis());

            long elapsedMs = System.currentTimeMillis() - startTime;

            // Prepare JOB_RESULT payload
            JsonObject resultPayload = new JsonObject();

            resultPayload.addProperty("jobId", jobId);
            resultPayload.add("result", finalMatrix);
            resultPayload.addProperty("elapsedMs", elapsedMs);

            // Create JOB_RESULT message
            Message resultMessage = new Message(
                    Message.JOB_RESULT,
                    "coordinator",
                    resultPayload);

            // Send result to the original Client
            ClientConnection clientConnection = clientConnections.remove(jobId);

            if (clientConnection != null) {

                boolean sent = clientConnection.send(resultMessage);

                if (sent) {
                    System.out.println(
                            "Final result sent to Client for job " + jobId);
                } else {
                    System.err.println(
                            "Failed to send result to Client for job " + jobId);
                }

                clientConnection.close();

            } else {
                System.err.println(
                        "No Client connection found for job " + jobId);
            }

            // Clean up completed job information
            jobStartTimes.remove(jobId);
            jobResults.remove(jobId);
            jobTaskAssignments.remove(jobId);
            completedJobs.remove(jobId);
        }

        dispatchAvailableTasks();
    }

    private void handleJobSubmission(Message message, Socket socket) {
        Object jobType = message.payload.get("jobType");
        if (!"MATRIX_MULTIPLY".equals(jobType)) {
            System.err.println("Unsupported job type: " + jobType);
            try {
                socket.close();
            } catch (IOException ignored) {
                // Ignore closure failures while rejecting the job.
            }
            return;
        }

        Object matrixA = message.payload.get("matrixA");
        Object matrixB = message.payload.get("matrixB");
        if (!(matrixA instanceof List<?>) || !(matrixB instanceof List<?>)) {
            System.err.println("JOB_SUBMIT requires matrixA and matrixB arrays");
            try {
                socket.close();
            } catch (IOException ignored) {
                // Ignore closure failures while rejecting the job.
            }
            return;
        }
        List<?> rowsA = (List<?>) matrixA;
        List<?> rowsB = (List<?>) matrixB;

        int columnsA;

        try {
            columnsA = validateMatrix(rowsA, "Matrix A");
            validateMatrix(rowsB, "Matrix B");

        } catch (IllegalArgumentException e) {

            System.err.println("Invalid job: " + e.getMessage());

            try {
                socket.close();
            } catch (IOException ignored) {
                // Ignore closure failures while rejecting the job.
            }

            return;
        }

        // Check matrix multiplication compatibility
        if (columnsA != rowsB.size()) {

            System.err.println("Invalid matrix dimensions!");
            System.err.println("Matrix A columns: " + columnsA);
            System.err.println("Matrix B rows: " + rowsB.size());
            System.err.println("Matrix A columns must equal Matrix B rows.");

            try {
                socket.close();
            } catch (IOException ignored) {
                // Ignore closure failures while rejecting the job.
            }

            return;
        }

        System.out.println("Matrix dimensions validated successfully.");
        System.out.println("Matrix A: " + rowsA.size() + " x " + columnsA);
        System.out.println("Matrix B: " + rowsB.size() + " x " + ((List<?>) rowsB.get(0)).size());

        String jobId = "job-" + System.nanoTime();
        int totalWorkerCapacity = WorkerRegistry.getWorkerIds().stream()
                .mapToInt(WorkerRegistry::getMaxConcurrentChunks)
                .sum();
        int chunkCount = WorkerRegistry.getWorkerCount();

        if (chunkCount == 0) {
            chunkCount = 1;
        }
        // Create and save the Client connection for this job
        ClientConnection clientConnection;

        try {
            clientConnection = new ClientConnection(socket);
        } catch (IOException e) {
            System.err.println("Could not save Client connection: " + e.getMessage());

            try {
                socket.close();
            } catch (IOException ignored) {
                // Ignore closure failures.
            }

            return;
        }

        // Save the connection and job start time BEFORE dispatching tasks
        clientConnections.put(jobId, clientConnection);
        jobStartTimes.put(jobId, System.currentTimeMillis());

        // Create and store task chunks
        List<TaskQueue.TaskAssignment> tasks = taskQueue.submitTasks(jobId, rowsA, rowsB, chunkCount);

        jobTaskAssignments.put(jobId, tasks);

        System.out.println("Queued job " + jobId
                + " with " + tasks.size() + " task chunks");

        // Dispatch tasks to available Workers
        dispatchAvailableTasks();
    }

    private void retryAndRequeueTask(
            TaskQueue.TaskAssignment task,
            String reason) {

        if (task == null) {
            return;
        }

        // Increase the retry counter.
        // TaskQueue now allows unlimited retries.
        boolean retryAllowed = taskQueue.retryTask(task.getTaskId());

        if (retryAllowed) {

            int retryCount = taskQueue.getRetryCounts()
                    .getOrDefault(task.getTaskId(), 0);

            System.err.println(
                    "Requeuing task " + task.getTaskId()
                            + " | Attempt: " + retryCount
                            + " | Reason: " + reason);

            // Put the same task back into the pending queue.
            // Its taskId and row range remain unchanged.
            taskQueue.enqueueTask(task);

            // Try to assign it to an available Worker.
            dispatchAvailableTasks();

        } else {
            System.err.println(
                    "Task could not be requeued: " + task.getTaskId());
        }
    }

    private int validateMatrix(List<?> matrix, String matrixName) {

        if (matrix.isEmpty()) {
            throw new IllegalArgumentException(
                    matrixName + " must contain at least one row");
        }

        int columnCount = -1;

        for (int i = 0; i < matrix.size(); i++) {

            Object rowValue = matrix.get(i);

            if (!(rowValue instanceof List<?>)) {
                throw new IllegalArgumentException(
                        matrixName + " must contain only array rows");
            }

            List<?> row = (List<?>) rowValue;

            if (row.isEmpty()) {
                throw new IllegalArgumentException(
                        matrixName + " cannot contain empty rows");
            }

            if (columnCount == -1) {
                columnCount = row.size();
            } else if (row.size() != columnCount) {
                throw new IllegalArgumentException(
                        matrixName + " must be rectangular");
            }

            // Check that all values are integers
            for (Object value : row) {

                if (!(value instanceof Number)) {
                    throw new IllegalArgumentException(
                            matrixName + " must contain numeric values");
                }

                try {
                    java.math.BigDecimal number = new java.math.BigDecimal(value.toString());

                    if (number.stripTrailingZeros().scale() > 0) {
                        throw new IllegalArgumentException(
                                matrixName + " must contain integer values");
                    }

                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException(
                            matrixName + " contains an invalid number");
                }
            }
        }

        return columnCount;
    }

    private List<List<?>> asMatrixRows(List<?> resultRows) {
        List<List<?>> matrix = new ArrayList<>();
        for (Object row : resultRows) {
            if (!(row instanceof List<?>)) {
                throw new IllegalArgumentException("Each result row must be an array");
            }
            matrix.add((List<?>) row);
        }
        return matrix;
    }

    private List<List<?>> assembleJobResult(List<TaskQueue.TaskAssignment> tasksForJob,
            Map<String, List<List<?>>> partialResults) {
        int totalRows = 0;
        for (TaskQueue.TaskAssignment task : tasksForJob) {
            totalRows = Math.max(totalRows, task.getEndRow() + 1);
        }

        List<List<?>> assembled = new ArrayList<>(totalRows);
        for (int i = 0; i < totalRows; i++) {
            assembled.add(null);
        }

        for (TaskQueue.TaskAssignment task : tasksForJob) {
            List<List<?>> chunkRows = partialResults.get(task.getTaskId());
            if (chunkRows == null) {
                continue;
            }
            for (int offset = 0; offset < chunkRows.size(); offset++) {
                int rowIndex = task.getStartRow() + offset;
                if (rowIndex >= assembled.size()) {
                    break;
                }
                assembled.set(rowIndex, chunkRows.get(offset));
            }
        }

        List<List<?>> compact = new ArrayList<>();
        for (int i = 0; i < assembled.size(); i++) {
            if (assembled.get(i) == null) {
                throw new IllegalStateException(
                        "Missing result for matrix row " + i);
            }
        }

        return assembled;

    }

    public void handleWorkerDisconnect(String workerId) {
        workerLastHeartbeats.remove(workerId);

        Set<String> assignedTasks = workerAssignments.remove(workerId);

        if (assignedTasks == null || assignedTasks.isEmpty()) {
            System.out.println(
                    "No unfinished tasks to recover from " + workerId);

            dispatchAvailableTasks();
            return;
        }

        List<TaskQueue.TaskAssignment> tasksToRetry = new ArrayList<>();

        synchronized (assignedTasks) {
            for (String taskId : new ArrayList<>(assignedTasks)) {

                TaskQueue.TaskAssignment task = inFlightAssignments.remove(taskId);

                if (task != null) {
                    tasksToRetry.add(task);
                }
            }

            assignedTasks.clear();
        }

        System.out.println(
                "Recovering " + tasksToRetry.size()
                        + " unfinished tasks from " + workerId);

        for (TaskQueue.TaskAssignment task : tasksToRetry) {
            retryAndRequeueTask(
                    task,
                    "Worker " + workerId + " disconnected");
        }

        dispatchAvailableTasks();
    }

    private static final long HEARTBEAT_TIMEOUT_MS = 6000;

    private final Map<String, Long> workerLastHeartbeats = new ConcurrentHashMap<>();

    private final ScheduledExecutorService heartbeatMonitor = Executors.newSingleThreadScheduledExecutor();

    public void handleHeartbeat(String workerId) {
        workerLastHeartbeats.put(workerId, System.currentTimeMillis());
    }

    public void startHeartbeatMonitor() {

        heartbeatMonitor.scheduleAtFixedRate(() -> {

            long currentTime = System.currentTimeMillis();

            for (Map.Entry<String, Long> entry : workerLastHeartbeats.entrySet()) {

                String workerId = entry.getKey();
                long lastHeartbeat = entry.getValue();

                if (currentTime - lastHeartbeat > HEARTBEAT_TIMEOUT_MS) {

                    System.err.println(
                            "Worker " + workerId + " heartbeat timeout!");

                    WorkerHandler handler = WorkerRegistry.getHandler(workerId);

                    if (handler != null) {
                        handler.disconnect();
                    }
                }

            }

        }, 2, 2, TimeUnit.SECONDS);
    }

    private final Map<String, ClientConnection> clientConnections = new ConcurrentHashMap<>();

    private final Map<String, Long> jobStartTimes = new ConcurrentHashMap<>();

    private static class ClientConnection {

        private final Socket socket;
        private final PrintWriter out;

        public ClientConnection(Socket socket) throws IOException {
            this.socket = socket;
            this.out = new PrintWriter(socket.getOutputStream(), true);
        }

        public boolean send(Message message) {
            out.print(message.toWireString());
            out.flush();
            return !out.checkError();
        }

        public void close() {
            try {
                socket.close();
            } catch (IOException ignored) {
                // Ignore socket closure errors.
            }
        }
    }

}