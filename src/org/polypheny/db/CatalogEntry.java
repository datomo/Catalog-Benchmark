package org.polypheny.db;


import java.io.Serializable;


/**
 * Base class for a CatalogEntry
 */
public abstract class CatalogEntry {

    private final String name;


    CatalogEntry( String name ) {
        this.name = name;
    }


    public String getName() {
        return name;
    }

    public byte[] getBytes() {
        return name.getBytes();
    }
}
