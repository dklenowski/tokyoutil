package com.orbious.util.tokyo;

import java.io.File;

import com.orbious.util.config.Config;
import com.orbious.util.config.ConfigException;
import com.orbious.util.config.IConfig;

import tokyocabinet.HDB;

public class HDBMemStorage<K, V> extends MemStorage<K, V> implements IFileStorage {

  public HDBMemStorage(File filestore, Class<K> keytype, Class<V> valuetype,
      boolean readOnly) {
    super(filestore, HDB.class, keytype, valuetype, readOnly);
  }

  public void setDefaultFields() {
    logger.info("Setting default field " +  Config.config_hdb_key);
    fields.set(Config.config_hdb_key);
  }

  public void updateFields() { }

  public String cfg(IConfig key) {
    String cfgstr;

    cfgstr = cfgstr();
    if ( cfgstr == null ) {
      return null;
    }

    return Config.get(cfgstr, key);
  }

  /*
   * Interface methods.
   */

  public void writecfg() throws StorageException {
    if ( readOnly ) {
      throw new StorageException("Cannot write config to a readOnly filestore " +
          filestore.toString());
    }

    try {
      fields.set(Config.config_hdb_key, Config.xmlstr());
    } catch ( ConfigException ce ) {
      throw new StorageException("Failed to set config " + Config.config_hdb_key, ce);
    }
  }

  @Override
  public void close() throws StorageException {
    StorageException e = null;

    if ( !readOnly ) {
      try {
        writecfg();
      } catch ( StorageException se ) {
        e = se;
      }
    }

    super.close();

    if ( e != null ) {
      throw e;
    }
  }

  public String cfgstr() {
    return fields.get(Config.config_hdb_key);
  }
}
