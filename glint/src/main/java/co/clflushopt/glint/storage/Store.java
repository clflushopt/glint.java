package co.clflushopt.glint.storage;

import com.google.protobuf.ByteString;

/**
 * Allows users to specify a data storage API oblivious to the underlying
 * storage model.
 */
public interface Store {
    /**
     * Fetch a value by its key.
     * 
     * @param key
     * @return Value for key if one exists, null otherwise.
     */
    public ByteString get(ByteString key);

    /**
     * Write a key, value pair to the store and returns the old value if one exists;
     * otherwise `null`.
     * 
     * @param key
     * @param value
     * @return Old value if one exists, null otherwise.
     */
    public ByteString put(ByteString key, ByteString value);
}
