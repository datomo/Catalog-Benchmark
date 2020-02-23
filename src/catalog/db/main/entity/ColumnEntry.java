package catalog.db.main.entity;


import javafx.print.Collation;


public class ColumnEntry extends CatalogEntry {

    private static final long serialVersionUID = -6566756853822620430L;

    public final long id;
    public final String name;
    public final long tableId;
    public final String tableName;
    public final long schemaId;
    public final String schemaName;
    public final long databaseId;
    public final String databaseName;
    public final int position;
    public final Integer length; // JDBC length or precision depending on type
    public final Integer scale; // decimal digits
    public final boolean nullable;


    public ColumnEntry( long id, String name, long tableId, String tableName, long schemaId, String schemaName, long databaseId, String databaseName, int position, Integer length, Integer scale, boolean nullable) {
        this.id = id;
        this.name = name;
        this.tableId = tableId;
        this.tableName = tableName;
        this.schemaId = schemaId;
        this.schemaName = schemaName;
        this.databaseId = databaseId;
        this.databaseName = databaseName;
        this.position = position;
        this.length = length;
        this.scale = scale;
        this.nullable = nullable;
    }


    @Override
    public byte[] serialize() {
        return Serializer.serialize( this );
    }


    @Override
    public boolean equals( Object o ) {
        if (!(o instanceof ColumnEntry)) {
            return false;
        }
        ColumnEntry other = (ColumnEntry) o;
        return this.id ==  other.id;
    }


}
