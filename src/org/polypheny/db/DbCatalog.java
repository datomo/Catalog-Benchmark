package org.polypheny.db;


public interface DbCatalog {

    void addSchema( SchemaEntry schema );

    void addTable( TableEntry table );

    void addColumn( ColumnEntry column );

    SchemaEntry getSchema( String schema );

    TableEntry getTable( String schema, String table );

    ColumnEntry getColumn( String schema, String table, String column );

    void close();
}
