package catalog.db.main;


import catalog.db.main.entity.ColumnEntry;
import catalog.db.main.entity.SchemaEntry;
import catalog.db.main.entity.TableEntry;
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

    List<TableEntry> getTables( String schema );

    List<ColumnEntry> getColumns();

    List<ColumnEntry> getColumns( String schema );

    ColumnEntry getColumn( String schema, String table );

    Long getColumn( Long schemaId, Long tableId );

    boolean isClosed();

    void close();

    void clear();
}
