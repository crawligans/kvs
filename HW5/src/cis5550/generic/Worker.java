package cis5550.generic;

import java.net.URL;
import java.util.Random;
/*
* Your cis5550.kvs.Worker should accept three command-line arguments:
*  1) a port number for the worker,
* 2) a storage directory, and
* 3) the IP and port of the master, separated by a colon (:).
* When started, this application should look for a file called id in the storage directory;
* if it exists, it should read the worker’s ID from this file, otherwise it should pick an ID of five random lower-case letters and write it to the file.
* It should make a /ping request to the master every five seconds, and it should, via a web server that runs on the port number from the command line, make two functions available via HTTP:
*  1) A PUT to /data/<T>/<R>/<C> should set column C in row R of table T to the (possibly binary) data in the body of the request, and 2) a GET for /data/<T>/<R>/<C> should return the data in column C of row R in table T if the table, row, and column all exist; if not, it should return a 404 error. In both cases, the angular brackets denote arguments and should not appear in the URL; for instance, a GET to /data/foo/bar/xyz should return the xyz column of row bar in table foo. Row and column keys should be case-sensitive.
* */
public class Worker {
    /*public static void main(String[] args) throws Exception {
        if(args.length != 3){
            System.out.println("Usage: java cis5550.generic.Worker <port> <storage_dir> <master_ip:port>");
            System.exit(1);
        }
        startPingThread(args);
        Server.start(Integer.parseInt(args[0]));

    }*/
    protected static String id = null;

    public static void startPingThread(String[] args){
        new Thread(() -> {
            while(true){
                try {
                    Thread.sleep(5000);
                    final int SHORT_ID_LENGTH = 5;
                    if(id == null){
                        id = generateRandomString(SHORT_ID_LENGTH);
                    }
                    try{
                        URL url = new URL("http://" + args[2] + "/ping?id=" + id + "&port=" + args[0]);
                        url.getContent();
                    }catch(Exception e){
                        e.printStackTrace();
                    }

                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    protected static String generateRandomString(int short_id_length) {
        if(short_id_length < 1){
            return "";
        }
        String rd_str = "";
        for(int i = 0; i < short_id_length; i++){
            Random random = new Random();
            char cd  = (char) (random.nextInt(26) +  'a');

            rd_str += cd;
        }
        return rd_str;
    }
}
