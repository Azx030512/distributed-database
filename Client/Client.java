import java.io.IOException;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.net.URL;
import java.net.HttpURLConnection;
import java.util.Scanner;

public class Client {
    public static void main(String[] args) {
        Scanner input = new Scanner(System.in);
        String zookeeperHost = "127.0.0.1:2181";
        String tableName = input.next();

        RegionServerInfo regionServerInfo = getRegionServer(zookeeperHost, tableName);

        if(regionServerInfo != null){
            getRpcFile(regionServerInfo.getIp(), regionServerInfo.getPort(), regionServerInfo.getPath());
        }
    }

    public static RegionServerInfo getRegionServer(String zookeeperHost, String tableName){
        RegionServerInfo regionServerInfo = null;

        try{
            URL url = new URL("http://" + zookeeperHost + "/get_region_server_ip");
            String params = "table_name = " + tableName;

            // get Connection with zookeeper
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setDoOutput(true);
            conn.getOutputStream().write(params.getBytes());

            int responseCode = conn.getResponseCode();
            if(responseCode == 200){
                regionServerInfo.setIp(conn.getHeaderField("ip"));
                regionServerInfo.setPort(conn.getHeaderField("port"));
                regionServerInfo.setPath(conn.getHeaderField("path"));
            }else {
                throw new Exception("Error: Failed to get region server information from Zookeeper.");
            }

        }catch (Exception e){
            e.printStackTrace();
        }

        return regionServerInfo;
    }

    public static void getRpcFile(String ip, String port, String path){
        try{
            URL url = new URL("http://" + ip + ":" + port + "/" + path);

            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");

            int responseCode = conn.getResponseCode();
            if(responseCode == 200){
                File file = new File("rpc_file");
                BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(file));

                // user while to write file down
                byte[] buffer = new byte[1024];
                int bytesRead;
                while ((bytesRead = conn.getInputStream().read(buffer)) != -1) {
                    out.write(buffer, 0, bytesRead);
                }

                out.close();
            }else {
                throw new Exception("Error: Failed to get file from RegionServer.");
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public class RegionServerInfo {
        private String ip;
        private String port;
        private String path;

        public RegionServerInfo(String ip, String port, String path){
            this.ip = ip;
            this.port = port;
            this.path = path;
        }

        public String getIp() {
            return ip;
        }

        public void setIp(String ip) {
            this.ip = ip;
        }

        public String getPort() {
            return port;
        }

        public void setPort(String port) {
            this.port = port;
        }

        public String getPath() {
            return path;
        }

        public void setPath(String path) {
            this.path = path;
        }
    }

}
