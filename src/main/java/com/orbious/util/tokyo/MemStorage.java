package com.orbious.util.tokyo;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import org.apache.log4j.Logger;
import com.orbious.util.Bytes;
import com.orbious.util.Loggers;
import com.orbious.util.tokyo.DBFields.KeyBytes;

import tokyocabinet.DBM;

public abstract class MemStorage<K, V> implements IStorage {

  protected final File filestore;
  protected final Class<?> filetype;
  protected final Class<K> keytype;
  protected final Class<V> valuetype;
  protected final boolean readOnly;
  protected HashMap<K, V> map;
  protected DBM dbm;
  protected DBFields fields;
  protected Logger logger;

  public abstract void setDefaultFields();
  public abstract void updateFields();

  public MemStorage(File filestore, Class<?> filetype, Class<K> keytype,
      Class<V> valuetype, boolean readOnly) {
    this.filestore = filestore;
    this.filetype = filetype;
    this.keytype = keytype;
    this.valuetype = valuetype;
    this.readOnly = readOnly;
    this.map = new HashMap<K,V>();
    this.logger = Loggers.logger();

    this.fields = new DBFields();
    setDefaultFields();
  }

  public MemStorage(String filename, Class<?> filetype, Class<K> keytype,
      Class<V> valuetype, boolean readOnly) {
    this(new File(filename), filetype, keytype, valuetype, readOnly);
  }

  public boolean exists() {
    return true;
  }

  public boolean isopen() {
    if ( dbm != null ) {
      return true;
    }

    return false;
  }

  public boolean readOnly() {
    return readOnly;
  }

  public String path() {
    return filestore.toString();
  }

  public void open() throws StorageException {
    byte[] key;

    if ( readOnly && !filestore.exists() ) {
      throw new StorageException("File does not exist, cannot open readOnly?");
    }

    try {
      dbm = Helper.open(filestore, filetype, -1, false);
    } catch ( HelperException he ) {
      throw new StorageException("Failed to open " + filestore.toString(), he);
    }

    dbm.iterinit();
    while ( (key = dbm.iternext()) != null ) {
      if ( fields.contains(key) ) {
        fields.set(key, dbm.get(key));
        continue;
      }
      map.put(keytype.cast(Bytes.deserialize(key)),
          valuetype.cast(Bytes.deserialize(dbm.get(key))));
    }

    updateFields();
    logger.info("Loaded " + map.size() + " keys from " + filestore.toString());
  }

  public Iterator<K> iterator() {
    return map.keySet().iterator();
  }

  public void setField(String key) {
    fields.set(key);
  }

  public void setField(String key, String value) {
    fields.set(key, value);
  }

  public String getField(String key) {
    return fields.get(key);
  }

  public long size() {
    return map.size();
  }

  public void clear() {
    map.clear();
  }

  public void close() throws StorageException {
    Iterator<K> it;
    K key;
    HashMap<KeyBytes, byte[]> entries;
    Iterator<KeyBytes> it2;
    KeyBytes kb;

    if ( !readOnly ) {
      it = map.keySet().iterator();
      while ( it.hasNext() ) {
        key = it.next();
        dbm.put(Bytes.serialize(key), Bytes.serialize(map.get(key)));
      }


      entries = fields.entries();
      it2 = entries.keySet().iterator();
      while ( it2.hasNext() ) {
        kb = it2.next();
        dbm.put(kb.buffer(), entries.get(kb));
      }
    }

    try {
      Helper.close(dbm);
      dbm = null;
    } catch ( HelperException he ) {
      throw new StorageException("Error closing " + filestore.toString(), he);
    }
  }

  public V get(K key) {
    return map.get(key);
  }

  public V put(K key, V value) {
    return map.put(key, value);
  }

}
