package catalog.db.main.entity;


public class DatabaseEntry extends CatalogEntry {

    private static final long serialVersionUID = 4711611630126858410L;

    public final long id;
    public final String name;
    public final int ownerId;
    public final String ownerName;
    public final Long defaultSchemaId; // can be null
    public final String defaultSchemaName; // can be null


    public DatabaseEntry( long id, String name, int ownerId, String ownerName, Long defaultSchemaId, String defaultSchemaName ) {
        this.id = id;
        this.name = name;
        this.ownerId = ownerId;
        this.ownerName = ownerName;
        this.defaultSchemaId = defaultSchemaId;
        this.defaultSchemaName = defaultSchemaName;
    }


    @Override
    byte[] serialize() {
        return Serializer.serialize(this);
    }


    public boolean equals( Object o ) {
        if ( !(o instanceof DatabaseEntry) ) {
            return false;
        }
        DatabaseEntry other = (DatabaseEntry) o;
        return this.id == other.id;
    }
}
