package cis5550.kvs;

import cis5550.webserver.Server;

import java.io.*;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

public class Worker extends cis5550.generic.Worker {

    private static ConcurrentHashMap< String, Table > tables = new ConcurrentHashMap< String , Table >();
    private static String filePath = null;
    public static void main(String[] args) {
        if(args.length != 3){
            System.out.println("Usage: java cis5550.generic.Worker <port> <storage-directory> <ip:port of master>");
            System.exit(1);
        }
        int port = Integer.parseInt(args[0]);
        Server.port(port);
        startPingThread(args);
        registerID(args);
        readTableLogs();
        addPutRoute();
        addGetRoute();
        addGetDefault();
        addTableViewer();
        addPersist();
        addGetRow();
        addGetTable();
        addPutTable();
        addPutRename();
        addGetCount();
    }

    private static void addGetCount() {
        Server.get("/count/:T", (req, res) -> {
            String tableID = req.params("T");
            if (tableID == null) {
                res.status(400, "Bad Request");
                return "FAIL";
            }
            if (!tables.containsKey(tableID)) {
                res.status(404, "Not Found");
                return "Table Not Found";
            }
            res.type("text/plain");
            Table table = tables.get(tableID);
            return table.size();
        });
    }

    private static void addPutRename() {
        Server.put("/rename/:T", ((request, response) -> {
            String tableID = request.params("T");
            if (tableID == null) {
                response.status(400, "Bad Request");
                return "FAIL";
            }
            if (!tables.containsKey(tableID)) {
                response.status(404, "Not Found");
                return "Table Not Found";
            }
            String newTableID = request.body();
            if (newTableID == null) {
                response.status(400, "Bad Request");
                return "FAIL";
            }
            if (tables.containsKey(newTableID)) {
                response.status(400, "Bad Request");
                return "FAIL";
            }

            Table table = tables.get(tableID);
            table.rename(newTableID);
            tables.remove(tableID);
            tables.put(newTableID, table);
            return "OK";
        }));

    }

    private static void addPutTable() {
        Server.put("/data/:T", (req, res) -> {
            String tableID = req.params("T");
            if (tableID == null) {
                res.status(400, "Bad Request");
                return "FAIL";
            }
            if (!tables.containsKey(tableID)) {
               tables.put(tableID, new Table(tableID));
            }
            ByteArrayInputStream bais = new ByteArrayInputStream(req.bodyAsBytes());
            while (bais.available() > 0) {
                try {
                    Row row = Row.readFrom(bais);
                    tables.get(tableID).putRow(row);
                } catch (Exception e) {
                    res.status(400, "Bad Request");
                    return "FAIL";
                }
            }
            return "OK";
        });
    }

    private static void addGetTable() {
        Server.get("/data/:table", (req, res) -> {
            String tableID = req.params("table");
            if (tableID == null) {
                res.status(400, "Bad Request");
                return "FAIL";
            }
            if (!tables.containsKey(tableID)) {
                res.status(404, "Not Found");
                return "FAIL";
            }
            String startRow = req.queryParams("startRow");
            String endRowExclusive = req.queryParams("endRowExclusive");
            for (String row : tables.get(tableID).keySet()) {

                if (startRow != null && row.compareTo(startRow) < 0) {
                    continue;
                }
                if (endRowExclusive != null && row.compareTo(endRowExclusive) >= 0) {
                    continue;
                }
                try{
                    res.write(tables.get(tableID).getRow(row).toByteArray());
                    res.write("\n".getBytes());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            try{
                res.write("\n".getBytes());
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        });
    }

    private static void readTableLogs()  {
        for(File f : new File(filePath).listFiles()){
            if(f.getName().endsWith(".table")){
                try{
                    String tableName = f.getName().substring(0, f.getName().length() - 6);
                    Table t = new PersistentTable(tableName, filePath);
                    FileInputStream fp = new FileInputStream(f);
                    while(true){
                        Row r = Row.readFrom(fp);
                        if(r == null)
                            break;
                        t.putRow(r);
                    }
                    tables.put(tableName, t);
                }catch(Exception e){
                    e.printStackTrace();
                }
            }
        }
    }

    private static void registerID(String[] args) {
        File file = new File(args[1] + "/id");
        filePath = args[1];
        try{
            if(file.exists() && file.isFile()){
                System.out.println("File exists");  //debug
                BufferedReader br = new BufferedReader(new FileReader(file));
                String temp;
                if((temp = br.readLine()) != null){
                    id = temp;
                }
            }
            else if(file.isDirectory()){
                System.exit(1);
            }else{
                System.out.println(file.getAbsolutePath());
                file.createNewFile();
                id = generateRandomString(5);
                new FileWriter(file).write(id);
            }
            System.out.println(id);
        }catch(Exception e){
            e.printStackTrace();
        }
    }
    public static void addPutRoute(){
        Server.put("/data/:T/:R/:C", (request, response) -> {
            String table = request.params("T");
            String row = request.params("R");
            String column = request.params("C");
            String ifColumn = request.queryParams("ifcolumn");
            String equals = request.queryParams("equals");
            boolean exists = equals != null && ifColumn != null;
            byte[] body = request.bodyAsBytes();

            if(exists){
                if(!ifColumnCheck(table, row, ifColumn, equals)){
                    return "FAIL";
                }
            }
            if(!tables.containsKey(table)){
                tables.put(table, new Table(table));
            }
            Table tableMap = tables.get(table);

            if(!tableMap.containsKey(row)){
                tableMap.putRow(new Row(row));
            }
            Row rowMap = tableMap.getRow(row);
            System.out.println(rowMap.toString());
            rowMap.put(column, body);
            tableMap.putRow(rowMap);

            return "OK";

        });

    }

    private static boolean ifColumnCheck(String table, String row, String ifColumn, String equals) {
        if(!tables.containsKey(table)){
            return false;
        }
        Table tableMap = tables.get(table);
        if(!tableMap.containsKey(row)){
            return false;
        }
        Row rowMap = tableMap.getRow(row);
        if(!rowMap.containsKey(ifColumn)){
            return false;
        }
        if(rowMap.get(ifColumn) == null){
            return false;
        }
        return rowMap.get(ifColumn).equals(equals);
    }

    public static void addGetRoute(){
        Server.get("/data/:T/:R/:C", (request, response) -> {
            String table = request.params("T");
            String row = request.params("R");
            String column = request.params("C");
            if(!tables.containsKey(table)){
                response.status(404, "Not Found");
                return null;
            }
            Table tableMap = tables.get(table);
            if(!tableMap.containsKey(row)){
                response.status(404, "Not Found");
                return null;
            }
            Row rowMap = tableMap.getRow(row);
            if(!rowMap.containsKey(column)){
                response.status(404, "Not Found");
                return null;
            }
            response.bodyAsBytes(rowMap.getBytes(column));
            return null;
        });
    }
    private static String boilerPlate(String content, String title ){
        String boilerPlate = "<!DOCTYPE html>\n" +
                "<html>\n" +
                "<head>\n" +
                "    <meta charset=\"UTF-8\">\n" +
                "    <meta http-equiv=\"X-UA-Compatible\" content=\"IE=edge\">\n" +
                "    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">\n" +
                "    <title>" + (title == null ? "KVS Master" : title) + "</title>\n" +
                "</head>\n" +
                "<body>\n" +
                content +
                "</body>\n" +
                "</html>";
        return boilerPlate;
    }
    public static void addGetDefault(){
        Server.get("/", (request, response) -> {

            String allTables = "<h1>Tables</h1>\n";
            for (String id : tables.keySet()) {
                // create table with all ids of tables
                String s = "<table>\n" +
                        "    <tr>\n" +
                        "        <th>Table</th>\n" +
                        "        <th>Row</th>\n" +
                        "        <th>Column</th>\n" +
                        "        <th>Value</th>\n" +
                        "    </tr>\n";
                Table table = tables.get(id);
                for (String row : table.keySet()) {
                    Row rowMap = table.getRow(row);
                    for (String column : rowMap.columns()) {
                        s += "    <tr>\n" +
                                "        <td>" + id + "</td>\n" +
                                "        <td>" + row + "</td>\n" +
                                "        <td>" + column + "</td>\n" +
                                "        <td>" + rowMap.get(column) + "</td>\n" +
                                "    </tr>\n";
                    }
                }
                allTables += s;
            }

            return boilerPlate(allTables, null);
        });
    }
    public static void addTableViewer(){
        Server.get("/view/:X", (request, response) -> {
            String table = request.params("X");
            if(!tables.containsKey(table)){
                response.status(404, "Not Found");
            }
            Table tableMap = tables.get(table);
            int page = request.queryParams("page") == null ? 1 : Integer.parseInt(request.queryParams("page"));

            Row[] rows = tableMap.values().stream().sorted().skip((page - 1) * 10L).limit(10).toArray(Row[]::new);
            String[] columns = Arrays.stream(rows).flatMap(row -> row.columns().stream()).
                    distinct().sorted().limit(10).toArray(String[]::new);

            StringBuilder sb = new StringBuilder();


            sb.append("<table>");
            sb.append("<tr><th>row key</th>");
            for (String column : columns) {
                sb.append("<th>").append(column).append("</th>");
            }
            sb.append("</tr>");
            // rows
            for (Row row : rows) {
                sb.append("<tr>");
                sb.append("<td>").append((row).key()).append("</td>");
                for (String column : columns) {
                    sb.append("<td>").append((row).get(column)).append("</td>");
                }
                sb.append("</tr>");
            }
            sb.append("</table>");

            if (page * 10 < tableMap.size()){
                sb.append("<a href=/view/").append(table).append("?page=").append(page + 1).append(">Next</a>");
            }
            String fullHTML = boilerPlate(sb.toString(), table);
            response.type("text/html");
            return fullHTML;
        });

    }

    public static void addPersist(){
        Server.put("/persist/:X", (request, response) -> {
            String table = request.params("X");
            if((new File(filePath + "/" + table + ".table")).exists()){
                response.status(403, "Bad Request");
                return "FAIL";
            }else{
                tables.put(table , (new PersistentTable(table, filePath)));
            }
            return "OK";
        });
    }
    public static void addGetRow(){
        Server.get("/data/:T/:R", (req, res) ->{
            String table = req.params("T");
            String row = req.params("R");
            if (table == null || row == null) {
                res.status(400, "Bad Request");
                return "FAIL";
            }
            if (!tables.containsKey(table)) {
                res.status(404, "Not Found");
                return "FAIL";
            }
            Table t = tables.get(table);
            if (!t.containsKey(row)) {
                res.status(404, "Not Found");
                return "FAIL";
            }
            res.bodyAsBytes(tables.get(table).getRow(row).toByteArray());
            return null;
        });
    }

    /*
    Now add support for logging. Since each persistent table needs its own log, you’ll need to store a mapping from (persistent) tables to some object that wraps the log – perhaps a RandomAccessFile (since we’ll need to get rows from random file positions later on).
    Don’t worry about reading the logs for now; just open a new log when a persistent table is first created, and append a new entry as specified whenever something is added or changed. If you followed our recommendations on the HW4 handout,
    you should already have a putRow helper function; you can just add a write there if the table is persistent.
    Notice that the Row object already contains a toByteArray() method for serialization; all you need to do is add the final LF.
    To test, use KVSClient’s persist command to create a persistent table, then use the put command to generate some load; be sure to both insert new rows
and to change existing ones. You should see a new line appear in the relevant .table file for each addition or change. Also, the persist test case should now work.
     */

}
