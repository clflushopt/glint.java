package co.clflushopt.glint.storage;

import java.util.concurrent.ConcurrentHashMap;

import com.google.protobuf.ByteString;

/**
 * Implementation of the key-value store API using an in-memory data structure.
 */
public class MemoryStore implements Store {
    /**
     * In-memory store implementation using a Java native concurrent hashmap.
     */
    private final ConcurrentHashMap<ByteString, ByteString> store;

    /**
     * Creates a new instance of `InMemoryStore` with no initial capacity.
     */
    public MemoryStore() {
        this.store = new ConcurrentHashMap<>();
    }

    /**
     * Creates a new instance of `InMemoryStore` with `size` as initial capcity.
     * 
     * @param size
     */
    public MemoryStore(int size) {
        this.store = new ConcurrentHashMap<>(size);
    }

    /**
     * Returns the value for the matching `key`, this method is thread-safe.
     */
    @Override
    public ByteString get(ByteString key) {
        return this.store.get(key);
    }

    /**
     * Stores a new key-value pair, returning the old value for the key if one
     * already exists, this method is thread-safe.
     */
    @Override
    public ByteString put(ByteString key, ByteString value) {
        return this.store.put(key, value);
    }

    public int size() {
        return this.store.size();
    }

}
