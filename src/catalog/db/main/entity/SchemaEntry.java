package catalog.db.main.entity;


public class SchemaEntry extends CatalogEntry {

    private static final long serialVersionUID = 6130781950959616712L;

    public final long id;
    public final String name;
    public final long databaseId;
    public final String databaseName;
    public final int ownerId;
    public final String ownerName;


    public SchemaEntry( long id, String name, long databaseId, String databaseName, int ownerId, String ownerName ) {
        this.id = id;
        this.name = name;
        this.databaseId = databaseId;
        this.databaseName = databaseName;
        this.ownerId = ownerId;
        this.ownerName = ownerName;
    }


    @Override
    public byte[] serialize() {
        return GenericSerializer.serialize( this );
    }

    @Override
    public boolean equals( Object o ) {
        if ( !(o instanceof SchemaEntry) ) {
            return false;
        }
        SchemaEntry other = (SchemaEntry) o;
        return this.id == other.id;
    }
}
