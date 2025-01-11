package co.clflushopt.glint.storage;

import java.util.concurrent.ConcurrentHashMap;

public class MemoryStore implements Store {

    // Back this main memory store with a concurrent hashmap.
    public final ConcurrentHashMap<String, String> store;

    public MemoryStore() {
        this.store = new ConcurrentHashMap<>();
    }

    public MemoryStore(int size) {
        this.store = new ConcurrentHashMap<>(size);
    }

    @Override
    public String get(String key) {
        return this.store.get(key);
    }

    @Override
    public String put(String key, String value) {
        return this.store.put(key, value);
    }

    public int size() {
        return this.store.size();
    }

}
