package worker;

import java.io.*;
import java.net.*;

import protocol.Message;

public class WorkerTest {

    public static void main(String[] args) {

        String workerId = args.length > 0 ? args[0] : "worker_1";

        try (
                Socket socket = new Socket("localhost", 5000);

                PrintWriter out = new PrintWriter(
                        socket.getOutputStream(), true);

                BufferedReader in = new BufferedReader(
                        new InputStreamReader(socket.getInputStream()))) {

            // Send registration
            Message registerMessage = Message.registerWorker(workerId);

            out.println(registerMessage.toWireString());

            // Receive coordinator response
            String response = in.readLine();

            System.out.println("Coordinator response: " + response);

            // Keep connection alive for testing
            Thread.sleep(30000);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}