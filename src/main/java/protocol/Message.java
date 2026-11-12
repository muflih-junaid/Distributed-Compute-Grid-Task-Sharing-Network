package protocol;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

/**
 * The one shared representation of a message on the wire.
 * Coordinator, Worker, and Client code all import this class from the
 * shared `protocol` package — nobody hand-builds JSON strings themselves.
 *
 * Wire format (one line of JSON, newline-terminated):
 * {
 *   "type": "TASK_ASSIGN",
 *   "senderId": "coordinator",
 *   "payload": { ... }
 * }
 *
 * See PROTOCOL.md for the full list of message types and payload shapes.
 */
public class Message {

    // --- Message type constants: keep this list in sync with PROTOCOL.md ---
    public static final String REGISTER = "REGISTER";
    public static final String TASK_REQUEST = "TASK_REQUEST";
    public static final String HEARTBEAT = "HEARTBEAT";
    public static final String TASK_RESULT = "TASK_RESULT";
    public static final String TASK_ASSIGN = "TASK_ASSIGN";
    public static final String NO_TASK_AVAILABLE = "NO_TASK_AVAILABLE";
    public static final String JOB_SUBMIT = "JOB_SUBMIT";
    public static final String JOB_RESULT = "JOB_RESULT";

    private static final Gson GSON = new Gson();

    public String type;
    public String senderId;
    public JsonObject payload;

    public Message() {
        // no-arg constructor required by Gson
    }

    public Message(String type, String senderId, JsonObject payload) {
        this.type = type;
        this.senderId = senderId;
        this.payload = payload;
    }

    /** Serialize this message to a single JSON line, ready to write to a socket. */
    public String toWireString() {
        return GSON.toJson(this);
    }

    /** Parse one line read from a socket back into a Message. */
    public static Message fromWireString(String line) {
        return GSON.fromJson(line, Message.class);
    }

    // --- Convenience builders, so callers don't hand-build payloads either ---

    public static Message heartbeat(String workerId, int currentLoad) {
        JsonObject payload = new JsonObject();
        payload.addProperty("currentLoad", currentLoad);
        payload.addProperty("timestamp", System.currentTimeMillis());
        return new Message(HEARTBEAT, workerId, payload);
    }

    public static Message taskRequest(String workerId) {
        return new Message(TASK_REQUEST, workerId, new JsonObject());
    }
    public static Message registerWorker(String workerId) {

        JsonObject payload = new JsonObject();
        payload.addProperty("role", "WORKER");

        return new Message(REGISTER, workerId, payload);
    }

    // Add more builders here as you implement each message type —
    // e.g. taskAssign(...), taskResult(...), jobSubmit(...), jobResult(...).
    // Keeping them all in this one class is the point: one place to look,
    // one place to change, when the protocol evolves.
}
