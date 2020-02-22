package catalog.db.main;


import java.io.Serializable;


/**
 * Base class for a CatalogEntry
 */
public abstract class CatalogEntry implements Serializable {

    private final String name;


    CatalogEntry( String name ) {
        this.name = name;
    }


    public String getName() {
        return name;
    }


    /**
     * Transforms this object into byte form
     * @return the object in byte form
     */
    abstract byte[] serialize();

}
