package cis5550.kvs;

import java.util.concurrent.ConcurrentHashMap;

public class Table {
    protected String id;
    protected ConcurrentHashMap<String, Row> rows;
    public Table(String id){
        this.id = id;
        this.rows = new ConcurrentHashMap<String, Row>();
    }
    ConcurrentHashMap<String, Row > getTable(){
        return rows;
    }

    public boolean containsKey(String row) {
        return rows.containsKey(row);
    }
    public void putRow(Row row) {
        rows.put(row.key(), row);
    }
    public Row getRow(String row){
        return rows.get(row);
    }

    public String[] keySet() {
        return rows.keySet().toArray(new String[rows.size()]);
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
