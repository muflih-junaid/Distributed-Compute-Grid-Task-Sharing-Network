package protocol;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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

    public static final String REGISTER = "REGISTER";
    public static final String TASK_REQUEST = "TASK_REQUEST";
    public static final String HEARTBEAT = "HEARTBEAT";
    public static final String TASK_RESULT = "TASK_RESULT";
    public static final String TASK_ASSIGN = "TASK_ASSIGN";
    public static final String NO_TASK_AVAILABLE = "NO_TASK_AVAILABLE";
    public static final String JOB_SUBMIT = "JOB_SUBMIT";
    public static final String JOB_RESULT = "JOB_RESULT";
    public static class JsonObject {
        private final Map<String, Object> values = new LinkedHashMap<>();

        public JsonObject() {
        }

        public void addProperty(String key, String value) {
            values.put(key, value);
        }

        public void addProperty(String key, Number value) {
            values.put(key, value);
        }

        public void addProperty(String key, Boolean value) {
            values.put(key, value);
        }

        public void addProperty(String key, Character value) {
            values.put(key, value == null ? null : String.valueOf(value));
        }

        public Object get(String key) {
            return values.get(key);
        }

        public void add(String key, Object value) {
            values.put(key, value);
        }

        public String toJsonString() {
            StringBuilder builder = new StringBuilder();
            builder.append('{');
            boolean first = true;
            for (Map.Entry<String, Object> entry : values.entrySet()) {
                if (!first) {
                    builder.append(',');
                }
                first = false;
                builder.append(quoteJson(entry.getKey()))
                        .append(':')
                        .append(valueToJson(entry.getValue()));
            }
            builder.append('}');
            return builder.toString();
        }

        private static String valueToJson(Object value) {
            if (value == null) {
                return "null";
            }
            if (value instanceof String) {
                return quoteJson((String) value);
            }
            if (value instanceof Number || value instanceof Boolean) {
                return value.toString();
            }
            if (value instanceof Character) {
                return quoteJson(String.valueOf(value));
            }
            if (value instanceof JsonObject) {
                return ((JsonObject) value).toJsonString();
            }
            if (value instanceof List<?>) {
                StringBuilder builder = new StringBuilder("[");
                boolean first = true;
                for (Object element : (List<?>) value) {
                    if (!first) {
                        builder.append(',');
                    }
                    first = false;
                    builder.append(valueToJson(element));
                }
                return builder.append(']').toString();
            }
            throw new IllegalArgumentException("Unsupported JSON value: " + value.getClass().getName());
        }

        private static String quoteJson(String input) {
            if (input == null) {
                return "null";
            }
            StringBuilder builder = new StringBuilder();
            builder.append('"');
            for (int i = 0; i < input.length(); i++) {
                char ch = input.charAt(i);
                switch (ch) {
                    case '\\':
                        builder.append("\\\\");
                        break;
                    case '"':
                        builder.append("\\\"");
                        break;
                    case '\n':
                        builder.append("\\n");
                        break;
                    case '\r':
                        builder.append("\\r");
                        break;
                    case '\t':
                        builder.append("\\t");
                        break;
                    case '\b':
                        builder.append("\\b");
                        break;
                    case '\f':
                        builder.append("\\f");
                        break;
                    default:
                        if (ch < 0x20) {
                            builder.append(String.format("\\u%04x", (int) ch));
                        } else {
                            builder.append(ch);
                        }
                }
            }
            builder.append('"');
            return builder.toString();
        }
    }

    public String type;
    public String senderId;
    public JsonObject payload;

    public Message() {
    }

    public Message(String type, String senderId, JsonObject payload) {
        this.type = type;
        this.senderId = senderId;
        this.payload = payload;
    }

    public String toWireString() {
        StringBuilder builder = new StringBuilder();
        builder.append('{');
        builder.append("\"type\":").append(JsonObject.quoteJson(type));
        builder.append(',');
        builder.append("\"senderId\":").append(JsonObject.quoteJson(senderId));
        builder.append(',');
        builder.append("\"payload\":").append(payload == null ? "{}" : payload.toJsonString());
        builder.append('}');
        return builder.toString() + "\n";
    }

    public static Message fromWireString(String line) {
        if (line == null) {
            return null;
        }

        String trimmed = line.trim();
        if (trimmed.isEmpty()) {
            return null;
        }

        try {
            int[] index = {0};
            JsonObject root = parseJsonObject(trimmed, index);
            skipWhitespace(trimmed, index);
            if (index[0] != trimmed.length()) {
                return null;
            }

            Object typeValue = root.get("type");
            Object senderIdValue = root.get("senderId");
            Object payloadValue = root.get("payload");
            if (!(typeValue instanceof String) || !(senderIdValue instanceof String)
                    || !(payloadValue instanceof JsonObject)) {
                return null;
            }

            Message message = new Message();
            message.type = (String) typeValue;
            message.senderId = (String) senderIdValue;
            message.payload = (JsonObject) payloadValue;
            return message;
        } catch (IllegalArgumentException ex) {
            return null;
        }
    }

    private static JsonObject parseJsonObject(String json, int[] indexRef) {
        skipWhitespace(json, indexRef);
        if (indexRef[0] >= json.length() || json.charAt(indexRef[0]) != '{') {
            throw new IllegalArgumentException("Expected '{'");
        }
        indexRef[0]++;
        skipWhitespace(json, indexRef);

        JsonObject object = new JsonObject();
        if (indexRef[0] < json.length() && json.charAt(indexRef[0]) == '}') {
            indexRef[0]++;
            return object;
        }

        while (true) {
            skipWhitespace(json, indexRef);
            String key = readJsonString(json, indexRef);
            skipWhitespace(json, indexRef);
            expect(json, indexRef, ':');
            Object value = readJsonValue(json, indexRef);
            object.values.put(key, value);
            skipWhitespace(json, indexRef);
            if (indexRef[0] >= json.length()) {
                throw new IllegalArgumentException("Unexpected end of JSON");
            }
            char next = json.charAt(indexRef[0]);
            if (next == ',') {
                indexRef[0]++;
                continue;
            }
            if (next == '}') {
                indexRef[0]++;
                return object;
            }
            throw new IllegalArgumentException("Expected ',' or '}'");
        }
    }

    private static Object readJsonValue(String json, int[] indexRef) {
        skipWhitespace(json, indexRef);
        if (indexRef[0] >= json.length()) {
            throw new IllegalArgumentException("Unexpected end of JSON");
        }

        char c = json.charAt(indexRef[0]);
        if (c == '{') {
            return parseJsonObject(json, indexRef);
        }
        if (c == '[') {
            return parseJsonArray(json, indexRef);
        }
        if (c == '"') {
            return readJsonString(json, indexRef);
        }
        if (c == 't') {
            if (json.startsWith("true", indexRef[0])) {
                indexRef[0] += 4;
                return true;
            }
            throw new IllegalArgumentException("Invalid boolean value");
        }
        if (c == 'f') {
            if (json.startsWith("false", indexRef[0])) {
                indexRef[0] += 5;
                return false;
            }
            throw new IllegalArgumentException("Invalid boolean value");
        }
        if (c == 'n') {
            if (json.startsWith("null", indexRef[0])) {
                indexRef[0] += 4;
                return null;
            }
            throw new IllegalArgumentException("Invalid null value");
        }
        if (c == '-' || Character.isDigit(c)) {
            StringBuilder number = new StringBuilder();
            while (indexRef[0] < json.length()) {
                char current = json.charAt(indexRef[0]);
                if (Character.isDigit(current) || current == '-' || current == '+' || current == '.' || current == 'e'
                        || current == 'E') {
                    number.append(current);
                    indexRef[0]++;
                } else {
                    break;
                }
            }
            String text = number.toString();
            if (!text.matches("-?(0|[1-9][0-9]*)(\\.[0-9]+)?([eE][+-]?[0-9]+)?")) {
                throw new IllegalArgumentException("Invalid number");
            }
            return new BigDecimal(text);
        }
        throw new IllegalArgumentException("Unsupported JSON value at index " + indexRef[0]);
    }

    private static List<Object> parseJsonArray(String json, int[] indexRef) {
        expect(json, indexRef, '[');
        skipWhitespace(json, indexRef);
        List<Object> array = new ArrayList<>();
        if (indexRef[0] < json.length() && json.charAt(indexRef[0]) == ']') {
            indexRef[0]++;
            return array;
        }

        while (true) {
            array.add(readJsonValue(json, indexRef));
            skipWhitespace(json, indexRef);
            if (indexRef[0] >= json.length()) {
                throw new IllegalArgumentException("Unexpected end of JSON array");
            }
            char next = json.charAt(indexRef[0]);
            if (next == ',') {
                indexRef[0]++;
            } else if (next == ']') {
                indexRef[0]++;
                return array;
            } else {
                throw new IllegalArgumentException("Expected ',' or ']'");
            }
        }
    }

    private static String readJsonString(String json, int[] indexRef) {
        if (indexRef[0] >= json.length() || json.charAt(indexRef[0]) != '"') {
            throw new IllegalArgumentException("Expected a string value");
        }
        indexRef[0]++;
        StringBuilder builder = new StringBuilder();
        while (indexRef[0] < json.length()) {
            char ch = json.charAt(indexRef[0]++);
            if (ch == '"') {
                return builder.toString();
            }
            if (ch == '\\') {
                if (indexRef[0] >= json.length()) {
                    throw new IllegalArgumentException("Unterminated escape sequence");
                }
                char escaped = json.charAt(indexRef[0]++);
                switch (escaped) {
                    case '"':
                        builder.append('"');
                        break;
                    case '\\':
                        builder.append('\\');
                        break;
                    case '/':
                        builder.append('/');
                        break;
                    case 'b':
                        builder.append('\b');
                        break;
                    case 'f':
                        builder.append('\f');
                        break;
                    case 'n':
                        builder.append('\n');
                        break;
                    case 'r':
                        builder.append('\r');
                        break;
                    case 't':
                        builder.append('\t');
                        break;
                    case 'u':
                        if (indexRef[0] + 4 > json.length()) {
                            throw new IllegalArgumentException("Incomplete unicode escape");
                        }
                        String hex = json.substring(indexRef[0], indexRef[0] + 4);
                        builder.append((char) Integer.parseInt(hex, 16));
                        indexRef[0] += 4;
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported escape sequence \\" + escaped);
                }
                continue;
            }
            if (ch < 0x20) {
                throw new IllegalArgumentException("Unescaped control character in JSON string");
            }
            builder.append(ch);
        }
        throw new IllegalArgumentException("Unterminated string");
    }

    private static void skipWhitespace(String json, int[] indexRef) {
        while (indexRef[0] < json.length()) {
            char ch = json.charAt(indexRef[0]);
            if (Character.isWhitespace(ch)) {
                indexRef[0]++;
            } else {
                break;
            }
        }
    }

    private static void expect(String json, int[] indexRef, char expected) {
        skipWhitespace(json, indexRef);
        if (indexRef[0] >= json.length() || json.charAt(indexRef[0]) != expected) {
            throw new IllegalArgumentException("Expected '" + expected + "'");
        }
        indexRef[0]++;
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
        return registerWorker(workerId, 1);
    }

    public static Message registerWorker(String workerId, int maxConcurrentChunks) {
        JsonObject payload = new JsonObject();
        payload.addProperty("maxConcurrentChunks", maxConcurrentChunks);

        return new Message(REGISTER, workerId, payload);
    }

    public static Message taskAssign(String senderId, String workerId, String taskId, JsonObject payload) {
        JsonObject messagePayload = payload == null ? new JsonObject() : payload;
        messagePayload.addProperty("taskId", taskId);
        return new Message(TASK_ASSIGN, senderId, messagePayload);
    }

    public static Message taskResult(String senderId, String taskId, JsonObject payload) {
        JsonObject messagePayload = payload == null ? new JsonObject() : payload;
        messagePayload.addProperty("taskId", taskId);
        return new Message(TASK_RESULT, senderId, messagePayload);
    }

    public static Message noTaskAvailable(String workerId) {
        return new Message(NO_TASK_AVAILABLE, "coordinator", new JsonObject());
    }

    public static Message jobSubmit(String senderId, String jobId, JsonObject payload) {
        JsonObject messagePayload = payload == null ? new JsonObject() : payload;
        messagePayload.addProperty("jobId", jobId);
        return new Message(JOB_SUBMIT, senderId, messagePayload);
    }

    public static Message jobResult(String senderId, String jobId, JsonObject payload) {
        JsonObject messagePayload = payload == null ? new JsonObject() : payload;
        messagePayload.addProperty("jobId", jobId);
        return new Message(JOB_RESULT, senderId, messagePayload);
    }
}
