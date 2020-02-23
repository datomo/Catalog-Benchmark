package catalog.db.main.entity;


import java.io.Serializable;


/**
 * Base class for a CatalogEntry
 */
public abstract class CatalogEntry implements Serializable {


    /**
     * Transforms this object into byte form
     * @return the object in byte form
     */
    abstract byte[] serialize();

    /**
     * Overwrites equals from Object
     * @param o Object which is compared to
     * @return if both objects consist of the same
     */
    abstract public boolean equals(Object o);

}
