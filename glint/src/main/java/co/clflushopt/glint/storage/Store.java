package co.clflushopt.glint.storage;

/**
 * Allows users to specify a data storage API oblivious to the underlying
 * storage model.
 */
public interface Store {
    /**
     * Fetch a value by its key.
     * 
     * @param key
     * @return
     */
    public String get(String key);

    /**
     * Write a key, value pair to the store and returns the old value
     * if one exists; otherwise `null`.
     * 
     * @param key
     * @param value
     * @return
     */
    public String put(String key, String value);
}
