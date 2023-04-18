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
        if (FILE_PATH == null) {
            FILE_PATH = fp;
        }
        this.path = FILE_PATH + "/" + key + ".table";
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
            persistentRows.put(row.key(), file.length());
            file.seek(file.length());
            file.write(row.toByteArray());
            file.write('\n');
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void putRow(Row row, long offset) {
        if (unlinked) {
            throw new IllegalStateException();
        }
        persistentRows.put(row.key(), offset);
    }

    public void putRow(String rowKey, long offset) {
        if (unlinked) {
            throw new IllegalStateException();
        }
        persistentRows.put(rowKey, offset);
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
