package co.clflushopt.glint.storage;

import java.util.concurrent.ConcurrentHashMap;

import com.google.protobuf.ByteString;

public class MemoryStore implements Store {

    // Back this main memory store with a concurrent hashmap.
    public final ConcurrentHashMap<ByteString, ByteString> store;

    public MemoryStore() {
        this.store = new ConcurrentHashMap<>();
    }

    public MemoryStore(int size) {
        this.store = new ConcurrentHashMap<>(size);
    }

    @Override
    public ByteString get(ByteString key) {
        return this.store.get(key);
    }

    @Override
    public ByteString put(ByteString key, ByteString value) {
        return this.store.put(key, value);
    }

    public int size() {
        return this.store.size();
    }

}
