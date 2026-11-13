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

import protocol.Message;
import protocol.Message.JsonObject;

public class Coordinator implements Runnable {

    private final int port;
    private final TaskQueue taskQueue = new TaskQueue();
    private final Map<String, TaskQueue.TaskAssignment> inFlightAssignments = new ConcurrentHashMap<>();
    private final Map<String, Set<String>> workerAssignments = new ConcurrentHashMap<>();
    private final Map<String, List<TaskQueue.TaskAssignment>> jobTaskAssignments = new ConcurrentHashMap<>();
    private final Map<String, Map<String, List<List<?>>>> jobResults = new ConcurrentHashMap<>();

    public Coordinator(int port) {
        this.port = port;
    }

    @Override
    public void run() {
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

    private boolean dispatchNextTaskTo(String workerId, boolean notifyWhenUnavailable) {
        WorkerHandler workerHandler = WorkerRegistry.getHandler(workerId);
        if (workerHandler == null) {
            return false;
        }

        Set<String> assignedTasks = workerAssignments.computeIfAbsent(workerId,
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

            assignedTasks.add(task.getTaskId());
            inFlightAssignments.put(task.getTaskId(), task);
            workerHandler.sendMessage(task.toTaskAssignMessage(workerId));
            System.out.println("Assigned " + task.getTaskId() + " to worker " + workerId);
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
            System.err.println("Task " + taskId + " failed on worker " + workerId + ": " + payload.get("errorMessage"));
            taskQueue.enqueueTask(task);
            dispatchAvailableTasks();
            return;
        }

        Object resultValue = payload.get("result");
        if (!(resultValue instanceof List<?>)) {
            System.err.println("Task " + taskId + " completed without result rows");
            return;
        }

        String jobId = task.getJobId();
        Map<String, List<List<?>>> partialResults = jobResults.computeIfAbsent(jobId, ignored -> new ConcurrentHashMap<>());
        partialResults.put(taskId, asMatrixRows((List<?>) resultValue));

        List<TaskQueue.TaskAssignment> tasksForJob = jobTaskAssignments.get(jobId);
        if (tasksForJob != null && partialResults.size() == tasksForJob.size()) {
            List<List<?>> finalMatrix = assembleJobResult(tasksForJob, partialResults);
            System.out.println("Job " + jobId + " complete: " + finalMatrix);
            jobResults.remove(jobId);
            jobTaskAssignments.remove(jobId);
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

        String jobId = "job-" + System.nanoTime();
        int totalWorkerCapacity = WorkerRegistry.getWorkerIds().stream()
                .mapToInt(WorkerRegistry::getMaxConcurrentChunks)
                .sum();
        int chunkCount = Math.max(1, totalWorkerCapacity);
        List<TaskQueue.TaskAssignment> tasks = taskQueue.submitTasks(jobId, (List<?>) matrixA, (List<?>) matrixB, chunkCount);
        jobTaskAssignments.put(jobId, tasks);
        System.out.println("Queued job " + jobId + " with " + tasks.size() + " task chunks");
        dispatchAvailableTasks();
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
        for (List<?> row : assembled) {
            if (row != null) {
                compact.add((List<?>) row);
            }
        }
        return compact;
    }
}