package worker;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.UUID;

public final class WorkerMain {

    private WorkerMain() {
    }

    public static void main(String[] args) {
        String host = args.length > 0 ? args[0] : "localhost";
        int port = args.length > 1 ? parsePositiveInt(args[1], "port") : 5000;
        String workerId = args.length > 2 ? args[2] : "worker-" + UUID.randomUUID();
        int slots = args.length > 3 ? parsePositiveInt(args[3], "execution slots") : 2;

        Worker worker = new Worker(host, port, workerId, slots);
        Runtime.getRuntime().addShutdownHook(new Thread(worker::close, "worker-shutdown"));
        Thread console = new Thread(() -> watchForShutdown(worker), "worker-console");
        console.setDaemon(true);
        console.start();

        try {
            worker.run();
        } catch (IOException exception) {
            System.err.println("Worker connection failed: " + exception.getMessage());
            System.exit(1);
        }
    }

    private static void watchForShutdown(Worker worker) {
        try (BufferedReader console = new BufferedReader(new InputStreamReader(System.in))) {
            String line;
            while ((line = console.readLine()) != null) {
                if ("quit".equalsIgnoreCase(line.trim()) || "exit".equalsIgnoreCase(line.trim())) {
                    worker.close();
                    return;
                }
            }
        } catch (IOException exception) {
            System.err.println("Worker console input failed: " + exception.getMessage());
        }
    }

    private static int parsePositiveInt(String value, String name) {
        try {
            int parsed = Integer.parseInt(value);
            if (parsed > 0 && ("port".equals(name) ? parsed <= 65535 : true)) {
                return parsed;
            }
        } catch (NumberFormatException exception) {
            // Report the invalid command-line value below.
        }
        throw new IllegalArgumentException("Invalid " + name + ": " + value);
    }
}
