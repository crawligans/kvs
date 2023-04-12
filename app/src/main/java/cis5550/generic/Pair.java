package cis5550.generic;

public class Pair<T, T1> {
    private T key;
    private T1 value;
    private long timeCreated;

    public Pair(T key, T1 value) {
        this.key = key;
        this.value = value;
        this.timeCreated = System.currentTimeMillis();
    }
    T first(){
        return key;
    }
    T1 second(){
        return value;
    }
    long getTimeCreated(){
        return timeCreated;
    }
}

