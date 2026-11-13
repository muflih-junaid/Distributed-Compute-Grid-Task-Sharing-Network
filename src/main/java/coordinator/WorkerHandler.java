package coordinator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

import protocol.Message;
import protocol.Message.JsonObject;

public class WorkerHandler {

    private final Coordinator coordinator;
    private final Socket socket;
    private final BufferedReader in;
    private final Message registrationMessage;
    private final PrintWriter out;

    public WorkerHandler(Socket socket, BufferedReader in, Message message, Coordinator coordinator) {
        this.socket = socket;
        this.in = in;
        this.registrationMessage = message;
        this.coordinator = coordinator;
        try {
            this.out = new PrintWriter(socket.getOutputStream(), true);
        } catch (IOException exception) {
            throw new IllegalStateException("Unable to open output stream for worker", exception);
        }
    }

    public void handle() {
        String workerId = registrationMessage.senderId;

        try {
            int maxConcurrentChunks = readWorkerCapacity(registrationMessage.payload);
            boolean registered = WorkerRegistry.register(workerId, socket, maxConcurrentChunks);
            if (!registered) {
                socket.close();
                return;
            }
            WorkerRegistry.registerHandler(workerId, this);

            System.out.println(workerId + " registered successfully");
            JsonObject response = new JsonObject();
            response.addProperty("workerId", workerId);
            out.print(new Message("REGISTER_SUCCESS", "coordinator", response).toWireString());
            out.flush();
            System.out.println("Total registered workers: " + WorkerRegistry.getWorkerCount());

            String line;
            while ((line = in.readLine()) != null) {
                Message message = Message.fromWireString(line);
                if (message == null || message.type == null || message.payload == null) {
                    System.err.println("Ignoring invalid message from " + workerId);
                    continue;
                }

                switch (message.type) {
                    case Message.TASK_REQUEST:
                        coordinator.dispatchNextTaskTo(workerId);
                        break;

                    case Message.HEARTBEAT:
                        System.out.println(workerId + " heartbeat received");
                        break;

                    case Message.TASK_RESULT:
                        System.out.println(workerId + " task result received");
                        coordinator.handleTaskResult(workerId, message.payload);
                        break;

                    default:
                        System.out.println("Unknown worker message: " + message.type);
                        break;
                }
            }

        } catch (IOException e) {
            System.out.println(workerId + " disconnected");
        } finally {
            WorkerRegistry.remove(workerId, socket);
            try {
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static int readWorkerCapacity(JsonObject payload) {
        Object value = payload == null ? null : payload.get("maxConcurrentChunks");
        if (value == null) {
            return 1;
        }
        if (!(value instanceof Number)) {
            throw new IllegalArgumentException("maxConcurrentChunks must be a positive integer");
        }
        try {
            int capacity = new java.math.BigDecimal(value.toString()).intValueExact();
            if (capacity > 0) {
                return capacity;
            }
        } catch (NumberFormatException | ArithmeticException exception) {
            throw new IllegalArgumentException("maxConcurrentChunks must be a positive integer", exception);
        }
        throw new IllegalArgumentException("maxConcurrentChunks must be a positive integer");
    }

    public void sendMessage(Message message) {
        if (message == null) {
            return;
        }
        out.print(message.toWireString());
        out.flush();
    }
}