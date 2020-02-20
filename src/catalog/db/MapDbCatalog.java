package catalog.db;


import catalog.db.DbSerialize.ColumnSerializer;
import catalog.db.DbSerialize.ListSerializer;
import catalog.db.DbSerialize.SchemaSerializer;
import catalog.db.DbSerialize.TableSerializer;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;


/**
 * Database catalog which uses the schemaChildren and tableChildren maps as an index and the specific columns as information holder
 * this should help to reduce access of specific columns and table-/schema-specific requests
 */
public class MapDbCatalog implements DbCatalog {

    private final DB db;

    private final ConcurrentMap<String, SchemaEntry> schemas;
    private final ConcurrentMap<String, TableEntry> tables;
    private final ConcurrentMap<String, ColumnEntry> columns;

    private final ConcurrentMap<String, List<String>> schemaChildren;
    private final ConcurrentMap<String, List<String>> tableChildren;


    public MapDbCatalog() {
        db = DBMaker
                .memoryDB()
                .closeOnJvmShutdown()
                .make();

        this.schemas = db
                .hashMap( "schemas", Serializer.STRING, new SchemaSerializer() )
                .create();

        this.tables = db
                .hashMap( "tables", Serializer.STRING, new TableSerializer() )
                .create();

        this.columns = db
                .hashMap( "columns", Serializer.STRING, new ColumnSerializer() )
                .create();

        this.schemaChildren = db
                .hashMap( "schemaChildren", Serializer.STRING, new ListSerializer<>( Serializer.STRING ) )
                .create();

        this.tableChildren = db
                .hashMap( "tableChildren", Serializer.STRING, new ListSerializer<>( Serializer.STRING ) )
                .create();
    }


    @Override
    public void addSchema( SchemaEntry schema ) {
        this.schemaChildren.put( schema.getName(), ImmutableList.of() );
        this.schemas.put( schema.getName(), schema );
    }


    @Override
    public void addTable( TableEntry table ) {
        String schema = table.getSchema();
        List<String> tables = new ArrayList<>( this.schemaChildren.get( schema ) );
        tables.add( table.getName() );
        this.schemaChildren.put( schema, tables );
        this.tableChildren.put( schema + "." + table.getName(), ImmutableList.of() );
        this.tables.put( schema + "." + table.getName(), table );
    }


    @Override
    public void addColumn( ColumnEntry column ) {
        String schema = column.getSchema();
        String table = column.getTable();
        List<String> columns = new ArrayList<>( this.tableChildren.get( schema + "." + table ) );
        columns.add( column.getName() );
        this.tableChildren.put( schema + "." + table, columns );
        this.columns.put( schema + "." + table + "." + column.getName(), column );
    }


    @Override
    public SchemaEntry getSchema( String schema ) {
        return this.schemas.get( schema );
    }


    @Override
    public TableEntry getTable( String schema, String table ) {
        return this.tables.get( schema + "." + table );
    }


    @Override
    public ColumnEntry getColumn( String schema, String table, String column ) {
        return this.columns.get( schema + "." + table + "." + column );
    }


    @Override
    public List<String> getSchemaNames() {
        return new ArrayList<>( this.schemas.keySet() );
    }


    @Override
    public List<String> getTableNames() {
        return new ArrayList<>( this.tables.keySet() );
    }


    @Override
    public List<String> getColumnNames() {
        return new ArrayList<>( this.columns.keySet() );
    }


    @Override
    public List<SchemaEntry> getSchemas() {
        return new ArrayList<>( this.schemas.values() );
    }


    @Override
    public List<TableEntry> getTables() {
        return new ArrayList<>( this.tables.values() );
    }


    @Override
    public List<TableEntry> getTables( String schema ) {
        return null;
    }


    @Override
    public List<ColumnEntry> getColumns() {
        return new ArrayList<>( this.columns.values() );
    }


    @Override
    public List<ColumnEntry> getColumns( String schema ) {
        return null;
    }


    @Override
    public List<ColumnEntry> getColumn( String schema, String table ) {
        return null;
    }


    @Override
    public void close() {
        this.db.close();
    }


    class TableExistsException extends Exception {

        final String message = "table already exists in the catalog database";
    }
}
