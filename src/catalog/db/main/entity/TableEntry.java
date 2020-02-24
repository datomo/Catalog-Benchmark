package catalog.db.main.entity;


public class TableEntry extends CatalogEntry {

    private static final long serialVersionUID = 5426944084650275437L;

    public final long id;
    public final String name;
    public final long schemaId;
    public final String schemaName;
    public final long databaseId;
    public final String databaseName;
    public final int ownerId;
    public final String ownerName;
    public final String definition;
    public final Long primaryKey;


    public TableEntry( long id, String name, long schemaId, String schemaName, long databaseId, String databaseName, int ownerId, String ownerName, String definition, Long primaryKey ) {
        this.id = id;
        this.name = name;
        this.schemaId = schemaId;
        this.schemaName = schemaName;
        this.databaseId = databaseId;
        this.databaseName = databaseName;
        this.ownerId = ownerId;
        this.ownerName = ownerName;
        this.definition = definition;
        this.primaryKey = primaryKey;
    }


    @Override
    byte[] serialize() {
        return GenericSerializer.serialize( this );
    }

    @Override
    public boolean equals( Object o ) {
        if ( !(o instanceof TableEntry) ) {
            return false;
        }
        TableEntry other = (TableEntry) o;
        return this.id == other.id;
    }
}
