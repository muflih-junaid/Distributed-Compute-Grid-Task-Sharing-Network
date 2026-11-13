package coordinator;

import java.io.*;
import java.net.Socket;

import protocol.Message;

public class WorkerHandler {

    private final Socket socket;
    private final BufferedReader in;
    private final Message registrationMessage;

    public WorkerHandler(Socket socket, BufferedReader in, Message message) {

        this.socket = socket;
        this.in = in;
        this.registrationMessage = message;
    }

    public void handle() {

        String workerId = registrationMessage.senderId;

        try (PrintWriter out = new PrintWriter(
                socket.getOutputStream(), true)) {

            // Register worker
            boolean registered = WorkerRegistry.register(workerId, socket);

            if (!registered) {
                socket.close();
                return;
            }

            System.out.println(workerId + " registered successfully");
            out.println(
                    new Message("REGISTER_SUCCESS", "coordinator", new com.google.gson.JsonObject()).toWireString());
            System.out.println("Total registered workers: " + WorkerRegistry.getWorkerCount());

            // Listen for subsequent messages
            String line;

            while ((line = in.readLine()) != null) {

                Message message = Message.fromWireString(line);

                switch (message.type) {

                    case Message.HEARTBEAT:

                        System.out.println(workerId + " heartbeat received");
                        break;

                    case Message.TASK_RESULT:

                        System.out.println(workerId + " task result received");
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
}