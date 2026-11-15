
package client;

import protocol.Message;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Scanner;

public class Client {

    // Generate a matrix containing random integers from 1 to 10
    private static List<List<Integer>> generateRandomMatrix(
            int rows, int columns, Random random) {

        List<List<Integer>> matrix = new ArrayList<>();

        for (int i = 0; i < rows; i++) {
            List<Integer> row = new ArrayList<>();

            for (int j = 0; j < columns; j++) {
                int number = random.nextInt(10) + 1;
                row.add(number);
            }

            matrix.add(row);
        }

        return matrix;
    }

    // Read matrix values manually
    private static List<List<Integer>> readManualMatrix(
            int rows, int columns, Scanner scanner, String matrixName) {

        List<List<Integer>> matrix = new ArrayList<>();

        System.out.println("\nEnter values for Matrix " + matrixName + ":");

        for (int i = 0; i < rows; i++) {
            List<Integer> row = new ArrayList<>();

            for (int j = 0; j < columns; j++) {
                System.out.print(matrixName + "[" + i + "][" + j + "]: ");
                int number = scanner.nextInt();
                row.add(number);
            }

            matrix.add(row);
        }

        return matrix;
    }

    // Let the user choose manual or random input
    private static List<List<Integer>> createMatrix(
            int rows,
            int columns,
            Scanner scanner,
            Random random,
            String matrixName) {

        System.out.println("\nChoose input method for Matrix " + matrixName);
        System.out.println("1. Enter values manually");
        System.out.println("2. Generate random values");

        int choice;

        while (true) {
            System.out.print("Your choice: ");
            choice = scanner.nextInt();

            if (choice == 1 || choice == 2) {
                break;
            }

            System.out.println("Please enter 1 or 2.");
        }

        if (choice == 1) {
            return readManualMatrix(rows, columns, scanner, matrixName);
        } else {
            System.out.println("Generating Matrix " + matrixName + "...");

            return generateRandomMatrix(rows, columns, random);
        }
    }

    // Read a positive integer
    private static int readPositiveInt(Scanner scanner, String message) {

        int number;

        while (true) {
            System.out.print(message);
            number = scanner.nextInt();

            if (number > 0) {
                return number;
            }

            System.out.println("Please enter a number greater than zero.");
        }
    }

    // Display a matrix in a readable format
    private static void displayMatrix(
            List<List<Integer>> matrix, String matrixName) {

        System.out.println("\nMatrix " + matrixName + ":");

        for (List<Integer> row : matrix) {
            for (Integer value : row) {
                System.out.printf("%6d", value);
            }

            System.out.println();
        }
    }

    public static void main(String[] args) {

        Scanner scanner = new Scanner(System.in);
        Random random = new Random();

        System.out.println("====================================");
        System.out.println("   DISTRIBUTED MATRIX MULTIPLICATION");
        System.out.println("====================================");

        // Get Matrix A dimensions
        System.out.println("\nEnter Matrix A dimensions:");

        int rowsA = readPositiveInt(scanner, "Number of rows: ");
        int columnsA = readPositiveInt(scanner, "Number of columns: ");

        // Get Matrix B dimensions
        int rowsB;
        int columnsB;

        while (true) {
            System.out.println("\nEnter Matrix B dimensions:");

            rowsB = readPositiveInt(scanner, "Number of rows: ");
            columnsB = readPositiveInt(scanner, "Number of columns: ");

            // Matrix multiplication compatibility check
            if (columnsA == rowsB) {
                break;
            }

            System.out.println("\nInvalid matrix dimensions!");
            System.out.println("Matrix A has " + columnsA + " columns.");
            System.out.println("Matrix B must have " + columnsA + " rows.");
            System.out.println("Please enter Matrix B dimensions again.");
        }

        System.out.println("\nMatrix dimensions accepted.");
        System.out.println("Matrix A: " + rowsA + " x " + columnsA);
        System.out.println("Matrix B: " + rowsB + " x " + columnsB);
        System.out.println("Result:   " + rowsA + " x " + columnsB);

        // Create Matrix A
        List<List<Integer>> matrixA = createMatrix(
                rowsA, columnsA, scanner, random, "A");

        // Create Matrix B
        List<List<Integer>> matrixB = createMatrix(
                rowsB, columnsB, scanner, random, "B");

        // Display input matrices
        displayMatrix(matrixA, "A");
        displayMatrix(matrixB, "B");

        // Prepare JOB_SUBMIT message
        Message.JsonObject payload = new Message.JsonObject();

        payload.addProperty("jobType", "MATRIX_MULTIPLY");
        payload.add("matrixA", matrixA);
        payload.add("matrixB", matrixB);

        Message request = new Message(
                Message.JOB_SUBMIT,
                "client-1",
                payload
        );

        String host = "localhost";
        int port = 5001;

        // Connect to Coordinator and submit the job
        try (
                Socket socket = new Socket(host, port);

                PrintWriter out = new PrintWriter(
                        socket.getOutputStream(), true);

                BufferedReader in = new BufferedReader(
                        new InputStreamReader(socket.getInputStream()))
        ) {

            System.out.println("\nConnected to Coordinator.");

            out.print(request.toWireString());
            out.flush();

            System.out.println("JOB_SUBMIT sent successfully.");
            System.out.println("Waiting for calculation result...");

            // Wait until Coordinator sends JOB_RESULT
            String responseLine = in.readLine();

            if (responseLine == null) {
                System.out.println(
                        "Coordinator closed the connection without a result.");
                return;
            }

            Message response = Message.fromWireString(responseLine);

            if (response == null) {
                System.out.println("Invalid response received.");
                return;
            }

            if (Message.JOB_RESULT.equals(response.type)) {

                Object result = response.payload.get("result");
                Object elapsedMs = response.payload.get("elapsedMs");

                System.out.println("\n====================================");
                System.out.println("          FINAL RESULT");
                System.out.println("====================================");

                // Display the result matrix
                if (result instanceof List<?>) {
                    System.out.println("Result Matrix:");

                    for (Object rowObject : (List<?>) result) {

                        if (rowObject instanceof List<?>) {
                            for (Object value : (List<?>) rowObject) {
                                System.out.printf("%6s", value);
                            }

                            System.out.println();
                        }
                    }
                } else {
                    System.out.println(result);
                }

                System.out.println("\nCalculation time: "
                        + elapsedMs + " ms");

            } else {
                System.out.println(
                        "Unexpected response type: " + response.type);
            }

        } catch (IOException e) {

            System.out.println("\nCould not connect to Coordinator.");
            System.out.println("Error: " + e.getMessage());
        }

        scanner.close();
    }
}
