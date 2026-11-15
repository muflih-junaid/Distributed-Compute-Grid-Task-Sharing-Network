# Distributed Compute Grid

## Running worker nodes

From the repository root, compile the Java sources with the bundled Gson
library in PowerShell:

```powershell
javac -cp "lib\gson-2.13.2.jar" -d out (Get-ChildItem -Recurse src\main\java -Filter *.java | ForEach-Object FullName)
```

Start the coordinator in one terminal:

```powershell
java -cp "out;lib\gson-2.13.2.jar" coordinator.Main
```

Start each worker in a separate terminal, using a distinct ID:

```powershell
java -cp "out;lib\gson-2.13.2.jar" worker.WorkerMain localhost 5000 worker-1 2
java -cp "out;lib\gson-2.13.2.jar" worker.WorkerMain localhost 5000 worker-2 2
java -cp "out;lib\gson-2.13.2.jar" worker.WorkerMain localhost 5000 worker-3 2
java -cp "out;lib\gson-2.13.2.jar" worker.WorkerMain localhost 5000 worker-4 2
```

The worker arguments are coordinator host, port, worker ID, and execution
slots; all have defaults except the worker ID, which is generated when omitted.
Type `quit` or `exit` in a worker terminal to disconnect gracefully, or use
Ctrl+C.

Workers register using the `REGISTER` payload, request work with
`TASK_REQUEST`, send `HEARTBEAT` every two seconds, and return each chunk with
`TASK_RESULT`. The coordinator splits matrix jobs across the total number of
registered worker execution slots and can assign multiple chunks to a worker,
up to the slot count it registered. `TASK_ASSIGN.rowRange` is an inclusive
pair of row indices; `matrixA` contains exactly those rows and `matrixB` is
the full right-hand matrix. The coordinator reassembles results by row range.
`TASK_ACK` is not a message in `PROTOCOL.md`, so workers do not send it.

## Manually create and test a worker task

The manual test server acts as a small one-task coordinator. It accepts one
worker, prompts for a `TASK_ASSIGN` payload, sends it to the worker, and checks
that the returned `TASK_RESULT` matches the expected matrix product.

Compile the project, then use two terminals. Start the manual test server:

```powershell
java -cp "out;lib\gson-2.13.2.jar" worker.ManualTaskCoordinator 5001
```

In the second terminal, start a worker:

```powershell
java -cp "out;lib\gson-2.13.2.jar" worker.WorkerMain localhost 5001 manual-worker 1
```

When prompted in the first terminal, enter one JSON payload:

```json
{"taskId":"manual-1","rowRange":[0,1],"matrixA":[[1,2],[3,4]],"matrixB":[[5,6],[7,8]]}
```



## How to Compile and Run the Distributed Compute Grid

Follow these steps to compile and run the project on macOS.

### 1. Compile the Project

Open Terminal in the project root directory and execute:

```bash
javac -d out $(find src/main/java -name "*.java")
```

If compilation succeeds, proceed to the next steps.

### 2. Start the Coordinator

Open Terminal 1 and execute:

```bash
java -cp out coordinator.Main
```

The Coordinator will start listening for connections on port `5001`.

### 3. Start Worker 1

Open Terminal 2 and execute:

```bash
java -cp out worker.WorkerMain localhost 5001 worker-1 2
```

### 4. Start Worker 2

Open Terminal 3 and execute:

```bash
java -cp out worker.WorkerMain localhost 5001 worker-2 2
```

Additional Workers can be started using different Worker IDs. For example:

```bash
java -cp out worker.WorkerMain localhost 5001 worker-3 2
```

The final argument (`2`) specifies the maximum number of concurrent task chunks that the Worker can process.

### 5. Start the Client

Open Terminal 4 and execute:

```bash
java -cp out client.Client
```

The Client allows users to enter matrix dimensions and choose between manually entering matrix values or generating random values. It submits the matrix multiplication job to the Coordinator and receives the final result.

### Execution Order

Start the components in the following order:

1. Compile the project.
2. Start the Coordinator.
3. Start Worker 1.
4. Start Worker 2.
5. Start the Client.

**Important Notes:**
- Keep the Coordinator, Workers, and Client terminals open while the application is running.
- The Coordinator must be started before the Workers.
- Workers must connect to the Coordinator using the correct host and port.
- The Client can submit matrix multiplication jobs after the Coordinator is running.
- To apply source code changes, compile the project again before restarting the application.


The worker should display the task and result `[[19,22],[43,50]]`; the manual
test server prints `PASS` if the returned result matches. The test server
handles one worker and one task per run. Type `quit` in the worker terminal
after the test to stop it.