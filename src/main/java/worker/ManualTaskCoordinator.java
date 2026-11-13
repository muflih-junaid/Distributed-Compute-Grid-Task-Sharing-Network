package worker;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

import protocol.Message;
import protocol.Message.JsonObject;

public final class ManualTaskCoordinator {

    private static final String REGISTER_SUCCESS = "REGISTER_SUCCESS";

    private ManualTaskCoordinator() {
    }

    public static void main(String[] args) throws IOException {
        int port = args.length == 0 ? 5001 : parsePort(args[0]);
        run(port);
    }

    private static void run(int port) throws IOException {
        try (ServerSocket server = new ServerSocket(port)) {
            System.out.println("Manual task test server listening on port " + port);
            System.out.println("Start one worker with: worker.WorkerMain localhost " + port + " manual-worker 1");
            server.setSoTimeout(120000);

            try (Socket socket = server.accept()) {
                socket.setSoTimeout(30000);
                BufferedReader input = new BufferedReader(
                        new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
                PrintWriter output = new PrintWriter(socket.getOutputStream(), true, StandardCharsets.UTF_8);
                String workerId = registerWorker(input, output);
                System.out.println("Worker " + workerId + " connected; waiting for its task request.");

                waitForTaskRequest(input, workerId);
                ManualTask task = promptForTask();
                output.print(new Message(Message.TASK_ASSIGN, "coordinator", task.payload).toWireString());
                output.flush();
                if (output.checkError()) {
                    throw new IOException("Failed to send task assignment");
                }
                System.out.println("Sent task " + task.taskId + " to " + workerId);

                verifyTaskResult(input, workerId, task);
            } catch (SocketTimeoutException exception) {
                throw new IOException("Timed out waiting for a worker or task result", exception);
            }
        }
    }

    private static String registerWorker(BufferedReader input, PrintWriter output) throws IOException {
        Message registration = readMessage(input);
        if (registration == null || !Message.REGISTER.equals(registration.type)
                || registration.senderId == null || registration.senderId.trim().isEmpty()) {
            throw new IOException("Expected a valid REGISTER message from the worker");
        }

        JsonObject responsePayload = new JsonObject();
        responsePayload.addProperty("workerId", registration.senderId);
        output.print(new Message(REGISTER_SUCCESS, "coordinator", responsePayload).toWireString());
        output.flush();
        if (output.checkError()) {
            throw new IOException("Failed to acknowledge worker registration");
        }
        System.out.println("Registered worker " + registration.senderId);
        return registration.senderId;
    }

    private static void waitForTaskRequest(BufferedReader input, String workerId) throws IOException {
        while (true) {
            Message message = readMessage(input);
            if (message == null) {
                throw new IOException("Worker disconnected before requesting a task");
            }
            if (Message.TASK_REQUEST.equals(message.type)) {
                return;
            }
            if (Message.HEARTBEAT.equals(message.type)) {
                System.out.println("Heartbeat received from " + workerId);
            } else {
                System.out.println("Ignoring worker message before task request: " + message.type);
            }
        }
    }

    private static ManualTask promptForTask() throws IOException {
        BufferedReader console = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        while (true) {
            System.out.println("Enter a TASK_ASSIGN payload as one JSON object, for example:");
            System.out.println("{\"taskId\":\"manual-1\",\"rowRange\":[0,1],"
                    + "\"matrixA\":[[1,2],[3,4]],\"matrixB\":[[5,6],[7,8]]}");
            System.out.print("> ");
            String json = console.readLine();
            if (json == null) {
                throw new IOException("No task payload was entered");
            }

            ManualTask task = parseTask(json);
            if (task != null) {
                return task;
            }
            System.err.println("Invalid task payload or matrix dimensions. Please try again.");
        }
    }

    private static ManualTask parseTask(String json) {
        String envelope = "{\"type\":\"TASK_ASSIGN\",\"senderId\":\"coordinator\",\"payload\":"
                + json + "}";
        Message message = Message.fromWireString(envelope);
        if (message == null || !Message.TASK_ASSIGN.equals(message.type)) {
            return null;
        }

        Object taskIdValue = message.payload.get("taskId");
        Object matrixA = message.payload.get("matrixA");
        Object matrixB = message.payload.get("matrixB");
        Object rowRange = message.payload.get("rowRange");
        if (!(taskIdValue instanceof String) || ((String) taskIdValue).trim().isEmpty()
                || !(matrixA instanceof List<?>) || !(matrixB instanceof List<?>) || !(rowRange instanceof List<?>)) {
            return null;
        }

        try {
            List<?> expectedResult = ComputeEngine.multiply((List<?>) matrixA, (List<?>) matrixB, (List<?>) rowRange);
            return new ManualTask((String) taskIdValue, message.payload, expectedResult);
        } catch (IllegalArgumentException exception) {
            return null;
        }
    }

    private static void verifyTaskResult(BufferedReader input, String workerId, ManualTask task) throws IOException {
        while (true) {
            Message message = readMessage(input);
            if (message == null) {
                throw new IOException("Worker disconnected before returning TASK_RESULT");
            }
            if (Message.HEARTBEAT.equals(message.type)) {
                System.out.println("Heartbeat received from " + workerId);
                continue;
            }
            if (!Message.TASK_RESULT.equals(message.type)) {
                System.out.println("Ignoring worker message while waiting for result: " + message.type);
                continue;
            }

            Object resultTaskId = message.payload.get("taskId");
            Object success = message.payload.get("success");
            if (!task.taskId.equals(resultTaskId)) {
                throw new IOException("Received result for unexpected task: " + resultTaskId);
            }
            if (!Boolean.TRUE.equals(success)) {
                throw new IOException("Worker failed task " + task.taskId + ": "
                        + message.payload.get("errorMessage"));
            }

            Object actualResult = message.payload.get("result");
            if (!Objects.equals(task.expectedResult, actualResult)) {
                throw new IOException("Result mismatch. Expected " + task.expectedResult + " but received "
                        + actualResult);
            }
            System.out.println("PASS: worker " + workerId + " returned the expected result for " + task.taskId);
            System.out.println("Result: " + actualResult);
            return;
        }
    }

    private static Message readMessage(BufferedReader input) throws IOException {
        String line;
        while ((line = input.readLine()) != null) {
            Message message = Message.fromWireString(line);
            if (message == null || message.type == null || message.payload == null) {
                System.err.println("Ignoring invalid JSON message from worker");
                continue;
            }
            return message;
        }
        return null;
    }

    private static int parsePort(String value) {
        try {
            int port = Integer.parseInt(value);
            if (port > 0 && port <= 65535) {
                return port;
            }
        } catch (NumberFormatException exception) {
            // Report the invalid port below.
        }
        throw new IllegalArgumentException("Invalid TCP port: " + value);
    }

    private static final class ManualTask {
        private final String taskId;
        private final JsonObject payload;
        private final List<?> expectedResult;

        private ManualTask(String taskId, JsonObject payload, List<?> expectedResult) {
            this.taskId = taskId;
            this.payload = payload;
            this.expectedResult = expectedResult;
        }
    }
}
