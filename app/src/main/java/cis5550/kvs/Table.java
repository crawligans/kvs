package cis5550.kvs;

import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

public class Table {

    protected String id;
    protected Map<String, Row> rows;

    public Table(String id) {
        this.id = id;
        this.rows = new ConcurrentSkipListMap<>();
    }

    Map<String, Row> getTable() {
        return rows;
    }

    public boolean containsKey(String row) {
        return rows.containsKey(row);
    }

    public void putRow(Row row) {
        rows.put(row.key(), row);
    }

    public Row getRow(String row) {
        return rows.computeIfAbsent(row, Row::new);
    }

    public String[] keySet() {
        return rows.keySet().toArray(String[]::new);
    }

    public void rename(String newTableID) {
        this.id = newTableID;
    }
    public int size(){
        return rows.size();
    }

    public java.util.Collection<Row> values() {
        return rows.values();
    }
}
