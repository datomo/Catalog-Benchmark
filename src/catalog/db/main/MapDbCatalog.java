package catalog.db.main;


import catalog.db.main.DbSerialize.ColumnSerializer;
import catalog.db.main.DbSerialize.ListSerializer;
import catalog.db.main.DbSerialize.SchemaSerializer;
import catalog.db.main.DbSerialize.TableSerializer;
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

    private static final String FILE_PATH = "mapDB";
    private static DB db;

    private static ConcurrentMap<String, SchemaEntry> schemas;
    private static ConcurrentMap<String, TableEntry> tables;
    private static ConcurrentMap<String, ColumnEntry> columns;

    private static ConcurrentMap<String, List<String>> schemaChildren;
    private static ConcurrentMap<String, List<String>> tableChildren;
    private DB file;


    public MapDbCatalog() {
        this( FILE_PATH );
    }


    public MapDbCatalog( String path ) {
        if ( db != null && !db.isClosed() ) {
            return;
        } else if ( db != null ) {
            db.close();
        }

        // TODO: Mmap is faster but can cause crashes on 32 bit environments
        db = DBMaker
                .fileDB( path )
                .fileMmapEnable()
                .fileMmapEnableIfSupported()
                .fileMmapPreclearDisable()
                //.fileDeleteAfterOpen()
                .closeOnJvmShutdown()
                .make();

        initDBLayout( db );
    }


    private void initDBLayout( DB db ) {
        schemas = db.hashMap( "schemas", Serializer.STRING, new SchemaSerializer() ).createOrOpen();

        tables = db.hashMap( "tables", Serializer.STRING, new TableSerializer() ).createOrOpen();

        columns = db.hashMap( "columns", Serializer.STRING, new ColumnSerializer() ).createOrOpen();

        schemaChildren = db.hashMap( "schemaChildren", Serializer.STRING, new ListSerializer<>( Serializer.STRING ) ).createOrOpen();

        tableChildren = db.hashMap( "tableChildren", Serializer.STRING, new ListSerializer<>( Serializer.STRING ) ).createOrOpen();
    }


    private <A, B> ConcurrentMap<A, B> createMapIfNotExists( DB db, String name, Serializer<A> keySerializer, Serializer<B> valueSerializer ) {
        if( db.get( name ) == null ){
            return db.hashMap( name, keySerializer, valueSerializer ).create();
        }
        return db.get( name );

    }


    @Override
    public void addSchema( SchemaEntry schema ) {
        schemaChildren.put( schema.getName(), ImmutableList.of() );
        schemas.put( schema.getName(), schema );
    }


    @Override
    public void addTable( TableEntry table ) {
        String schema = table.getSchema();
        List<String> newTables = new ArrayList<>( schemaChildren.get( schema ) );
        newTables.add( table.getName() );
        schemaChildren.put( schema, newTables );
        tableChildren.put( schema + "." + table.getName(), ImmutableList.of() );
        tables.put( schema + "." + table.getName(), table );
    }


    @Override
    public void addColumn( ColumnEntry column ) {
        String schema = column.getSchema();
        String table = column.getTable();
        List<String> newColumns = new ArrayList<>( tableChildren.get( schema + "." + table ) );
        newColumns.add( column.getName() );
        tableChildren.put( schema + "." + table, newColumns );
        columns.put( schema + "." + table + "." + column.getName(), column );
    }


    @Override
    public SchemaEntry getSchema( String schema ) {
        return schemas.get( schema );
    }


    @Override
    public TableEntry getTable( String schema, String table ) {
        return tables.get( schema + "." + table );
    }


    @Override
    public ColumnEntry getColumn( String schema, String table, String column ) {
        return columns.get( schema + "." + table + "." + column );
    }


    @Override
    public List<String> getSchemaNames() {
        return new ArrayList<>( schemas.keySet() );
    }


    @Override
    public List<String> getTableNames() {
        return new ArrayList<>( tables.keySet() );
    }


    @Override
    public List<String> getColumnNames() {
        return new ArrayList<>( columns.keySet() );
    }


    @Override
    public List<SchemaEntry> getSchemas() {
        return new ArrayList<>( schemas.values() );
    }


    @Override
    public List<TableEntry> getTables() {
        return new ArrayList<>( tables.values() );
    }


    @Override
    public List<TableEntry> getTables( String schema ) {
        return null;
    }


    @Override
    public List<ColumnEntry> getColumns() {
        return new ArrayList<>( columns.values() );
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
    public boolean isClosed() {
        return db.isClosed();
    }


    @Override
    public void close() {
        // moveToDisk();

        // this.file.close();
        db.close();
    }


    @Override
    public void clear() {
        schemas.clear();
        schemaChildren.clear();
        tables.clear();
        tableChildren.clear();
        columns.clear();
    }


    /**
     * Creates the existing database layout on the file db and moves all the infos to it.
     */
    private void moveToDisk() {
        ConcurrentMap<String, SchemaEntry> schemasFile = createMapIfNotExists( this.file, "schemas", Serializer.STRING, new SchemaSerializer() );
        ConcurrentMap<String, TableEntry> tablesFile = createMapIfNotExists( this.file, "tables", Serializer.STRING, new TableSerializer() );
        ConcurrentMap<String, ColumnEntry> columnsFile = createMapIfNotExists( this.file, "columns", Serializer.STRING, new ColumnSerializer() );

        schemasFile.clear();
        tablesFile.clear();
        columnsFile.clear();

        schemas.forEach( schemasFile::put );
        tables.forEach( tablesFile::put );
        columns.forEach( columnsFile::put );
    }


    private void getFromDisk() {

    }


    static class TableExistsException extends Exception {

        final String message = "table already exists in the catalog database";
    }
}
