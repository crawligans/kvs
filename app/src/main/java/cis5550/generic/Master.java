package cis5550.generic;

import cis5550.webserver.Server;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class Master {
    private static ConcurrentHashMap<String, Pair<String, String> > activeWorkers = new ConcurrentHashMap<>();

    public static List<String> getWorkers(){
        return activeWorkers.keySet().stream().toList();
    }
    public static String workerTable(){
        String s = "<table>\n" +
                "    <tr>\n" +
                "        <th>Worker ID</th>\n" +
                "        <th>IP:Port</th>\n" +
                "        <th>Hyperlink</th>\n" +
                "    </tr>\n";
        for (String id : activeWorkers.keySet()) {
            Pair<String, String>  p = activeWorkers.get(id);
            if(p.getTimeCreated() > System.currentTimeMillis() - 15000){
                s += "    <tr>\n" +
                        "        <td>" + id + "</td>\n" +
                        "        <td>" + p.first() + ":" + p.second() + "</td>\n" +
                        "        <td><a href=\"http://" + p.first() + ":" + p.second() + "\">" + p.first() + ":" + p.second() + "</a></td>\n" +
                        "    </tr>\n";
            }else{
                activeWorkers.remove(id);
            }
        }
        s += "</table>";
        return s;
    }
    public static void registerRoutes() {
        Server.get("/ping", (req, res) -> {
            String id = req.queryParams("id");
            String port = req.queryParams("port");
            res.header("Content-Type", "text/plain");
            if (id == null || port == null) {
                res.status(400, "Invalid request");
            }
            String ip = req.ip();
            System.out.println("Received ping from worker " + id + " at " + ip + ":" + port);
            if (!activeWorkers.containsKey(id)) {
                activeWorkers.put(id, new Pair<>(ip, port));
            } else {
                activeWorkers.replace(id, new Pair<>(ip, port));
            }
            return "OK";
        });
        Server.get("/workers", (req, res) -> {
            String s = "";
            int size = 0;
            for (String id : activeWorkers.keySet()) {
                Pair<String, String>  p = activeWorkers.get(id);
                if(p.getTimeCreated() > System.currentTimeMillis() - 15000){
                    size++;
                    s += id + "," + p.first() + ":" + p.second() + "\n";
                }else{
                    activeWorkers.remove(id);
                }
            }
            s = size + "\n"+ s;
            return s;
        });
    }
}
