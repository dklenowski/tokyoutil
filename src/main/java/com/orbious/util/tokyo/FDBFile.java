package com.orbious.util.tokyo;

import java.io.File;

public class FDBFile {
  protected final File filestore;
  private final boolean readOnly;

//  protected FDBWrapper fdbw;

  public FDBFile(File filestore, boolean readOnly) {
    this.filestore = filestore;
    this.readOnly = readOnly;
  }

  public FDBFile(String name, int tokyoSize, boolean readOnly) {
    this(new File(name), readOnly);
  }
//
//
//  public void open() throws FileException {
//    if ( fdbw != null ) {
//      return;
//    }
//
//    fdbw = new FDBWrapper(filestore,readOnly);
//    try {
//      fdbw.open();
//    } catch ( WrapperException we ) {
//      throw new FileException("Failed to open sentence file" + filestore, we);
//    }
//  }
//
//  public void close() throws FileException {
//
//    try {
//      fdbw.close();
//    } catch ( WrapperException hwe ) {
//      throw new FileException("Error closing hdb file " + filestore, hwe);
//    }
//  }
//
//  public FDBWrapper hdbw() {
//    return fdbw;
//  }
//
//  @Override
//  public String toString() {
//    return filestore.toString();
//  }
}
