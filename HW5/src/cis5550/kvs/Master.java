package cis5550.kvs;


import cis5550.webserver.Server;



public class Master extends cis5550.generic.Master{

    public static void main(String[] args){
        if(args.length != 1) {
            System.out.println("Usage: java cis5550.kvs.Master <port>");
            System.exit(1);
        }
        Server.port(Integer.parseInt(args[0]));
        registerRoutes();
        Server.get("/", (req, res) -> "<!DOCTYPE html>\n" +
                "<html>\n" +
                "<head>\n" +
                "    <meta charset=\"UTF-8\">\n" +
                "    <meta http-equiv=\"X-UA-Compatible\" content=\"IE=edge\">\n" +
                "    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">\n" +
                "    <title>KVS Master</title>\n" +
                "</head>\n" +
                "<body>\n" +
                workerTable() +
                "</body>\n" +
                "</html>");

    }
}
