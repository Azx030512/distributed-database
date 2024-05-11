// 尚未完成
import java.io.*;
import java.net.Socket;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Scanner;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import com.google.gson.*;
import java.net.URI;

public class Client {
    private static final String MASTER_API = "http://127.0.0.1:5000/api/rounte/";
    private static final Gson gson = new Gson();
    private static final HashMap<String, String[]> cache = new HashMap<>();

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        String command;
        // 开始显示欢迎信息
        System.out.println("Welcome to the Java Client Shell!");
        while (true) {
            System.out.print("ClientShell> ");
            String input = scanner.nextLine();

            if (input.equals("quit")) {
                break;
            }

            List<String> inputParts = new ArrayList<>(Arrays.asList(input.split(" ")));
            String operation = inputParts.remove(0); // This is equivalent to JavaScript's shift()
            String argString = String.join(" ", inputParts); // This is equivalent to JavaScript's join(' ')

            switch (operation) {
                case "create":
                    String[] createArgs = argString.split(" ", 2);
                    String estimatedSize = createArgs[0];
                    String sqlStatement = createArgs[1];
                    if (estimatedSize == null || sqlStatement == null) {
                        System.out.println("Invalid argument. Usage: create <estimated_size> <sql_statement>");
                        break;
                    }
                    try {
                        int estimatedSizeInt = Integer.parseInt(estimatedSize);
                        if (estimatedSizeInt < 0) {
                            System.out.println("Invalid estimated size. Please use a positive number.");
                            break;
                        }
                    } catch (NumberFormatException e) {
                        System.out.println("Invalid estimated size. Please use a positive number.");
                        break;
                    }
                    String operationInSql = sqlStatement.split(" ")[0].toUpperCase();
                    if (!operationInSql.equals("CREATE")) {
                        System.out.println("Invalid operation. Please use CREATE with create.");
                        break;
                    }
                    String tableName = sqlStatement.split(" ")[2];
                    sendRequestToMaster("create_table", tableName, estimatedSize, sqlStatement);
                    break;
                    case "read":
                        String sqlStatement = args;
                        String operationInSql = sqlStatement.split(" ")[0].toUpperCase();
                        if (!operationInSql.equals("SELECT")) {
                            System.out.println("Invalid operation. Please use SELECT with read.");
                            break;
                        }
                        String tableName2 = sqlStatement.split(" ")[3];
                        if (cache.containsKey(tableName)) {
                            sendJsonToRegionServer("4", tableName2, sqlStatement, cache.get(tableName)[0]);
                        } else {
                            sendRequestToMaster("read_table", tableName2, null, sqlStatement);
                        }
                        break;
                case "write":
                    sqlStatement = argString;
                    operation = argString.split(" ")[0].toUpperCase();
                    tableName = argString.split(" ")[2];
                    if (cache.containsKey(tableName)) {
                        for (String address : cache.get(tableName)) {
                            sendJsonToRegionServer("3", tableName, sqlStatement, address);
                        }
                    } else {
                        sendRequestToMaster("write_table", tableName, sqlStatement);
                    }
                    break;
                case "drop":
                    tableName = argString;
                    sqlStatement = "DROP TABLE " + tableName;
                    if (cache.containsKey(tableName)) {
                        for (String address : cache.get(tableName)) {
                            sendJsonToRegionServer("2", tableName, sqlStatement, address);
                        }
                        cache.remove(tableName);
                    } else {
                        sendRequestToMaster("drop_table", tableName, sqlStatement);
                    }
                    break;
                default:
                    System.out.println("Unknown command.");
                    break;
            }

        }
    }

    private static void sendRequestToMaster(String operation, String tableName, String estimatedSize, String data) {
        try {
            URI uri = new URI(MASTER_API + operation);
            URL url = uri.toURL();
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setDoOutput(true);
    
            JsonObject jsonObject = new JsonObject();
            jsonObject.addProperty("table_name", tableName);
            if (estimatedSize != null) {
                jsonObject.addProperty("estimated_size", estimatedSize);
            }
            if (data != null) {
                jsonObject.addProperty("data", data);
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

            // Receive and handle the response from the region server
            InputStream inputStream = socket.getInputStream();
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            String response = bufferedReader.readLine();
            Gson gson = new Gson();
            JsonElement element = gson.fromJson(response, JsonElement.class);
            JsonObject response_data = element.getAsJsonObject();

            switch (response_data.get("success").getAsInt()) {
                case 0:
                    System.out.println("Operation failed: " + response_data.get("message").getAsString());
                    break;
                case 1:
                    System.out.println("Create operation succeeded: " + response_data.get("message").getAsString());
                    break;
                case 2:
                    System.out.println("Drop operation succeeded: " + response_data.get("message").getAsString());
                    break;
                case 3:
                    System.out.println("Write operation succeeded: " + response_data.get("message").getAsString());
                    break;
                case 4:
                    System.out.println("Read operation succeeded: " + response_data.get("message").getAsString());
                    System.out.println("Data: " + response_data.get("data").toString());
                    break;
            }

            socket.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}