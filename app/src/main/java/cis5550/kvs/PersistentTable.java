package cis5550.kvs;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

public class PersistentTable extends Table {
    private RandomAccessFile file;
    private static String FILE_PATH = null;
    private ConcurrentHashMap<String, Long > persistentRows; //key, seekVal

    public PersistentTable(String key, String fp){
        super(key);
        if(FILE_PATH == null)
            FILE_PATH = fp;
        try{
            file = new RandomAccessFile(FILE_PATH + "/" + key + ".table", "rw");
            this.persistentRows = new ConcurrentHashMap<String, Long>();
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public void putRow(Row row)  {
        try{
            long seekVal = 0;
            while(seekVal != file.length()){
                file.seek(seekVal);
                String temp = file.readLine();
                if(temp != null){
                    System.out.println(temp + " compare to " + row.key());
                    if(temp.startsWith(row.key())){
                        file.seek(seekVal);
                        break;
                    }
                }
                seekVal += temp.length() + 1;
            }
            file.write(row.toByteArray());
            file.write("\n".getBytes());
            persistentRows.put(row.key(), seekVal);
        }catch(Exception e){
            e.printStackTrace();
        }
    }
    @Override
    public Row getRow(String rowID) {
        Long seekVal = persistentRows.get(rowID);
        if (seekVal == null){
            return null;
        }
        try {
            file.seek(seekVal);
            Row r = Row.readFrom(file);
            System.out.println("Row ID: " + rowID + "Size: " + r.columns().size());
            return r;
        }catch(Exception e){
            e.printStackTrace();
        }
        return null;
    }
    @Override
    public boolean containsKey(String column) {
        return persistentRows.containsKey(column);
    }

    @Override
    public void rename(String newTableID) {
        File f = new File(FILE_PATH + "/" + this.id + ".table");
        f.renameTo(new File(FILE_PATH + "/" + newTableID + ".table"));
        this.id = newTableID;
    }

    @Override
    public int size(){
        return persistentRows.size();
    }

    @Override
    public java.util.Collection<Row> values() {
        ArrayList<Row> rows = new ArrayList<Row>();
        for(String key : persistentRows.keySet()){
            rows.add(getRow(key));
        }
        return rows;
    }

}
