import java.io.*;
import java.net.Socket;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Scanner;
import java.util.HashMap;
import com.google.gson.*;
import java.net.URI;

public class Client {
    private static final String MASTER_API = "http://127.0.0.1:5000/api/rounte/";
    private static final Gson gson = new Gson();
    private static final HashMap<String, String[]> cache = new HashMap<>();

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        String command;
        //  开始显示欢迎信息
        System.out.println("Welcome to the Java Client Shell!");
        while (true) {
            System.out.print("ClientShell> ");
            command = scanner.nextLine();

            if (command.equals("quit")) {
                break;
            }

            String[] splitCommand = command.split(" ");
            String operation = splitCommand[0];
            String tableName = splitCommand[1];

            switch (operation) {
                case "create":
                    String estimatedSize = splitCommand[2];
                    sendRequestToMaster("create_table", tableName, estimatedSize);
                    break;
                case "read":
                    if (cache.containsKey(tableName)) {
                        sendJsonToRegionServer("4", tableName, null, cache.get(tableName)[0]);
                    } else {
                        sendRequestToMaster("read_table", tableName, null);
                    }
                    break;
                case "write":
                    String newData = splitCommand[2];
                    if (cache.containsKey(tableName)) {
                        for (String address : cache.get(tableName)) {
                            sendJsonToRegionServer("3", tableName, newData, address);
                        }
                    } else {
                        sendRequestToMaster("write_table", tableName, newData);
                    }
                    break;
                case "drop":
                    if (cache.containsKey(tableName)) {
                        for (String address : cache.get(tableName)) {
                            sendJsonToRegionServer("2", tableName, null, address);
                        }
                        cache.remove(tableName);
                    } else {
                        sendRequestToMaster("drop_table", tableName, null);
                    }
                    break;
                default:
                    System.out.println("Unknown command.");
                    break;
            }
        }
    }

    private static void sendRequestToMaster(String operation, String tableName, String data) {
        try {
            URI uri = new URI(MASTER_API + operation);
            URL url = uri.toURL();
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setDoOutput(true);

            JsonObject jsonObject = new JsonObject();
            jsonObject.addProperty("table_name", tableName);
            if (data != null) {
                jsonObject.addProperty("estimated_size", data);
            }

            String jsonInputString = jsonObject.toString();
            try (OutputStream os = conn.getOutputStream()) {
                byte[] input = jsonInputString.getBytes("utf-8");
                os.write(input, 0, input.length);
            }

            try (BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream(), "utf-8"))) {
                StringBuilder response = new StringBuilder();
                String responseLine = null;
                while ((responseLine = br.readLine()) != null) {
                    response.append(responseLine.trim());
                }
                JsonObject responseJson = gson.fromJson(response.toString(), JsonObject.class);
                if (responseJson.get("signal").getAsString().equals("success")) {
                    JsonArray addresses = responseJson.get("addresses").getAsJsonArray();
                    String[] addressArray = new String[addresses.size()];
                    for (int i = 0; i < addresses.size(); i++) {
                        addressArray[i] = addresses.get(i).getAsString();
                        sendJsonToRegionServer(operation, tableName, data, addressArray[i]);
                    }
                    cache.put(tableName, addressArray);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void sendJsonToRegionServer(String operation, String tableName, String data, String address) {
        try {
            Socket socket = new Socket(address.split(":")[0], Integer.parseInt(address.split(":")[1]));
            OutputStream outputStream = socket.getOutputStream();
            PrintWriter printWriter = new PrintWriter(outputStream, true);

            JsonObject jsonObject = new JsonObject();
            if (data != null) {
                JsonObject dataObject = new JsonObject();
                dataObject.addProperty(tableName, data);
                jsonObject.add(operation, dataObject);
            } else {
                jsonObject.addProperty(operation, tableName);
            }

            printWriter.println(jsonObject.toString());
            socket.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}