package coordinator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import protocol.Message;
import protocol.Message.JsonObject;

public class TaskQueue {

    private final Queue<TaskAssignment> pendingTasks = new ConcurrentLinkedQueue<>();
    private final Map<String, TaskAssignment> taskLookup = new ConcurrentHashMap<>();
    private final Map<String, Integer> retryCounts = new ConcurrentHashMap<>();

    public static final class TaskAssignment {
        private final String jobId;
        private final String taskId;
        private final int startRow;
        private final int endRow;
        private final List<?> matrixA;
        private final List<?> matrixB;

        public TaskAssignment(String jobId, String taskId, int startRow, int endRow,
                List<?> matrixA, List<?> matrixB) {
            this.jobId = jobId;
            this.taskId = taskId;
            this.startRow = startRow;
            this.endRow = endRow;
            this.matrixA = Collections.unmodifiableList(new ArrayList<>(matrixA));
            this.matrixB = Collections.unmodifiableList(new ArrayList<>(matrixB));
        }

        public String getJobId() {
            return jobId;
        }

        public String getTaskId() {
            return taskId;
        }

        public int getStartRow() {
            return startRow;
        }

        public int getEndRow() {
            return endRow;
        }

        public List<?> getMatrixA() {
            return matrixA;
        }

        public List<?> getMatrixB() {
            return matrixB;
        }

        public List<Integer> getRowRange() {
            return List.of(startRow, endRow);
        }

        public JsonObject toPayload() {
            JsonObject payload = new JsonObject();
            payload.add("rowRange", getRowRange());
            payload.add("matrixA", matrixA);
            payload.add("matrixB", matrixB);
            return payload;
        }

        public Message toTaskAssignMessage(String workerId) {
            return Message.taskAssign("coordinator", workerId, taskId, toPayload());
        }

        @Override
        public String toString() {
            return "TaskAssignment{" + "taskId='" + taskId + '\'' + ", jobId='" + jobId + '\''
                    + ", rowRange=[" + startRow + ", " + endRow + "]" + '}';
        }
    }

    public String submitJob(String jobId, List<?> matrixA, List<?> matrixB) {
        return submitJob(jobId, matrixA, matrixB, Math.max(1, matrixA == null ? 1 : matrixA.size()));
    }

    public String submitMatrixMultiplyJob(String jobId, List<?> matrixA, List<?> matrixB) {
        return submitJob(jobId, matrixA, matrixB);
    }

    public String enqueueJob(String jobId, List<?> matrixA, List<?> matrixB) {
        return submitJob(jobId, matrixA, matrixB);
    }

    public List<TaskAssignment> submitTasks(String jobId, List<?> matrixA, List<?> matrixB, int chunkCount) {
        if (matrixA == null || matrixB == null) {
            throw new IllegalArgumentException("Job matrices are required");
        }
        if (matrixA.isEmpty()) {
            throw new IllegalArgumentException("matrixA must contain at least one row");
        }
        if (chunkCount < 1) {
            throw new IllegalArgumentException("chunkCount must be positive");
        }

        String resolvedJobId = (jobId == null || jobId.trim().isEmpty()) ? "job-" + System.nanoTime() : jobId;
        int rowCount = matrixA.size();
        int effectiveChunkCount = Math.min(Math.max(1, chunkCount), rowCount);
        int rowsPerChunk = rowCount / effectiveChunkCount;
        int remainder = rowCount % effectiveChunkCount;

        List<TaskAssignment> tasks = new ArrayList<>();
        int startRow = 0;
        for (int index = 0; index < effectiveChunkCount; index++) {
            int chunkSize = rowsPerChunk + (index < remainder ? 1 : 0);
            int endRow = startRow + chunkSize - 1;
            List<Object> chunkRows = new ArrayList<>();
            for (int rowIndex = startRow; rowIndex <= endRow; rowIndex++) {
                chunkRows.add(matrixA.get(rowIndex));
            }
            String taskId = "chunk-" + resolvedJobId + "-" + index;
            TaskAssignment task = new TaskAssignment(resolvedJobId, taskId, startRow, endRow, chunkRows, matrixB);
            pendingTasks.add(task);
            taskLookup.put(taskId, task);
            retryCounts.put(taskId, 0);
            tasks.add(task);
            startRow = endRow + 1;
        }
        return tasks;
    }

    public String submitJob(String jobId, List<?> matrixA, List<?> matrixB, int chunkCount) {
        String resolvedJobId = (jobId == null || jobId.trim().isEmpty()) ? "job-" + System.nanoTime() : jobId;
        submitTasks(resolvedJobId, matrixA, matrixB, chunkCount);
        return resolvedJobId;
    }

    public void enqueueTask(TaskAssignment task) {
        if (task == null) {
            return;
        }
        pendingTasks.add(task);
        taskLookup.put(task.getTaskId(), task);
    }

    public boolean addTask(TaskAssignment task) {
        enqueueTask(task);
        return true;
    }

    public TaskAssignment pollNextTask() {
        TaskAssignment task = pendingTasks.poll();
        if (task == null) {
            return null;
        }
        taskLookup.remove(task.getTaskId());
        return task;
    }

    public TaskAssignment takeNextTask() {
        return pollNextTask();
    }

    public TaskAssignment nextTask() {
        return pendingTasks.peek();
    }

    public TaskAssignment peek() {
        return pendingTasks.peek();
    }

    public boolean hasPendingTasks() {
        return !pendingTasks.isEmpty();
    }

    public boolean isEmpty() {
        return pendingTasks.isEmpty();
    }

    public int size() {
        return pendingTasks.size();
    }

    public TaskAssignment getTask(String taskId) {
        return taskLookup.get(taskId);
    }

    public void markTaskCompleted(String taskId) {
        retryCounts.remove(taskId);
    }

    public boolean retryTask(String taskId, int maxRetries) {
        Integer count = retryCounts.get(taskId);
        int nextRetryCount = count == null ? 1 : count + 1;
        retryCounts.put(taskId, nextRetryCount);
        return nextRetryCount <= maxRetries;
    }

    public boolean retryTask(String taskId) {
        return retryTask(taskId, 3);
    }

    public Map<String, Integer> getRetryCounts() {
        return new HashMap<>(retryCounts);
    }
}
