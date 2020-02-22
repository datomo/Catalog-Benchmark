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

    private ConcurrentMap<String, SchemaEntry> schemas;
    private ConcurrentMap<String, TableEntry> tables;
    private ConcurrentMap<String, ColumnEntry> columns;

    private ConcurrentMap<String, List<String>> schemaChildren;
    private ConcurrentMap<String, List<String>> tableChildren;
    private DB file;


    public MapDbCatalog() {
        this( FILE_PATH );
    }


    public MapDbCatalog( String path ) {
        if( db != null ){
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

        //file.getAllNames().forEach( System.out::println );


        /*db = DBMaker
                //.memoryDB()
                .fileDB( F )
                .closeOnJvmShutdown()
                .make();
        this.db.*/

        initDBLayout( db );
    }


    private void initDBLayout( DB db ) {
        this.schemas = createMapIfNotExists( db, "schemas", Serializer.STRING, new SchemaSerializer() );

        this.tables = createMapIfNotExists( db, "tables", Serializer.STRING, new TableSerializer() );

        this.columns = createMapIfNotExists( db, "columns", Serializer.STRING, new ColumnSerializer() );

        this.schemaChildren = createMapIfNotExists( db, "schemaChildren", Serializer.STRING, new ListSerializer<>( Serializer.STRING ) );

        this.tableChildren = createMapIfNotExists( db, "tableChildren", Serializer.STRING, new ListSerializer<>( Serializer.STRING ) );
    }


    private <A, B> ConcurrentMap<A, B> createMapIfNotExists( DB db, String name, Serializer<A> keySerializer, Serializer<B> valueSerializer ) {
        return db.hashMap( name, keySerializer, valueSerializer ).createOrOpen();
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
    public boolean isClosed() {
        return this.db.isClosed();
    }


    @Override
    public void close() {
        // moveToDisk();

        // this.file.close();
        this.db.close();
    }


    @Override
    public void clear() {
        this.schemas.clear();
        this.schemaChildren.clear();
        this.tables.clear();
        this.tableChildren.clear();
        this.columns.clear();
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
