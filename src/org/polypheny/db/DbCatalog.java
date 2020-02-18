package org.polypheny.db;


import java.util.List;


public interface DbCatalog {

    void addSchema( SchemaEntry schema );

    void addTable( TableEntry table );

    void addColumn( ColumnEntry column );

    SchemaEntry getSchema( String schema );

    TableEntry getTable( String schema, String table );

    ColumnEntry getColumn( String schema, String table, String column );

    List<String> getSchemaNames();

    List<String> getTableNames();

    List<String> getColumnNames();

    List<SchemaEntry> getSchemas();

    List<TableEntry> getTables();

    List<ColumnEntry> getColumns();



    void close();
}
