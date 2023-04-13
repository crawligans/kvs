package cis5550.kvs;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

public class PersistentTable extends Table {

    private RandomAccessFile file;
    private static String FILE_PATH = null;
    private final String path;
    private ConcurrentHashMap<String, Long> persistentRows; //key, seekVal
    private boolean unlinked = false;

    public PersistentTable(String key, String fp) {
        super(key);
        this.path = FILE_PATH + "/" + key + ".table";
        if (FILE_PATH == null) {
            FILE_PATH = fp;
        }
        try {
            file = new RandomAccessFile(this.path, "rw");
            this.persistentRows = new ConcurrentHashMap<String, Long>();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Collection<Row> drop() throws IOException {
        unlinked = true;
        Collection<Row> rows = values();
        file.close();
        File f = new File(path);
        f.delete();
        return rows;
    }

    @Override
    public void putRow(Row row) {
        if (unlinked) {
            throw new IllegalStateException();
        }
        try {
            long seekVal = 0;
            while (seekVal != file.length()) {
                file.seek(seekVal);
                String temp = file.readLine();
                if (temp != null) {
                    System.out.println(temp + " compare to " + row.key());
                    if (temp.startsWith(row.key())) {
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
        if (unlinked) {
            throw new IllegalStateException();
        }
        Long seekVal = persistentRows.get(rowID);
        if (seekVal == null) {
            return null;
        }
        try {
            file.seek(seekVal);
            Row r = Row.readFrom(file);
            System.out.println("Row ID: " + rowID + "Size: " + r.columns().size());
            return r;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
    @Override
    public boolean containsKey(String column) {
        if (unlinked) {
            throw new IllegalStateException();
        }
        return persistentRows.containsKey(column);
    }

    @Override
    public void rename(String newTableID) {
        if (unlinked) {
            throw new IllegalStateException();
        }
        File f = new File(FILE_PATH + "/" + this.id + ".table");
        f.renameTo(new File(FILE_PATH + "/" + newTableID + ".table"));
        this.id = newTableID;
    }

    @Override
    public int size() {
        if (unlinked) {
            throw new IllegalStateException();
        }
        return persistentRows.size();
    }

    @Override
    public java.util.Collection<Row> values() {
        if (unlinked) {
            throw new IllegalStateException();
        }
        ArrayList<Row> rows = new ArrayList<Row>();
        for (String key : persistentRows.keySet()) {
            rows.add(getRow(key));
        }
        return rows;
    }

}
