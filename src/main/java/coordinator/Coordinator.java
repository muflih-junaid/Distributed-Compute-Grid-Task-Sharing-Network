package coordinator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

import protocol.Message;

public class Coordinator implements Runnable {

    private final int port;

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

            BufferedReader in = new BufferedReader(
                    new InputStreamReader(socket.getInputStream()));

            String line = in.readLine();

            if (line == null) {
                socket.close();
                return;
            }

            Message message = Message.fromWireString(line);

            switch (message.type) {

                case Message.REGISTER:

                    WorkerHandler workerHandler = new WorkerHandler(socket, in, message);

                    workerHandler.handle();
                    break;

                case Message.JOB_SUBMIT:

                    // ClientHandler will handle this later.
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
}