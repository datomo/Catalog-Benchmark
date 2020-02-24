package catalog.db.main;


import catalog.db.main.entity.ColumnEntry;
import catalog.db.main.entity.GenericSerializer;
import catalog.db.main.entity.SchemaEntry;
import catalog.db.main.entity.TableEntry;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;


/**
 * Database catalog which uses the schemaChildren and tableChildren maps as an index and the specific columns as information holder
 * this should help to reduce access of specific columns and table-/schema-specific requests
 */
public class MapDbCatalog implements DbCatalog {

    private static final String FILE_PATH = "mapDB";
    private static DB db;

    private static HTreeMap<String, SchemaEntry> schemas;
    private static HTreeMap<String, TableEntry> tables;
    private static HTreeMap<String, ColumnEntry> columns;

    private static HTreeMap<String, ImmutableList<String>> schemaChildren;
    //private static NavigableSet<Object[]> schemaChildren;
    private static HTreeMap<String, ImmutableList<String>> tableChildren;
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
        //db = DBMaker.memoryDB().make();

        initDBLayout( db );
    }


    private void initDBLayout( DB db ) {
        schemas = db.hashMap( "schemas", Serializer.STRING, new GenericSerializer<SchemaEntry>() ).createOrOpen();

        tables = db.hashMap( "tables", Serializer.STRING, new GenericSerializer<TableEntry>() ).createOrOpen();

        columns = db.hashMap( "columns", Serializer.STRING, new GenericSerializer<ColumnEntry>() ).createOrOpen();

        schemaChildren = db.hashMap( "schemaChildren", Serializer.STRING, new GenericSerializer<ImmutableList<String>>() ).createOrOpen();

        tableChildren = db.hashMap( "tableChildren", Serializer.STRING, new GenericSerializer<ImmutableList<String>>() ).createOrOpen();

        /*schemaChildren = db.treeSet("towns")
                //set tuple serializer
                .serializer(new SerializerArrayTuple(Serializer.STRING, Serializer.STRING))
                .counterEnable()
                .counterEnable()
                .counterEnable()
                .createOrOpen();*/
    }


    @Override
    public void addSchema( SchemaEntry schema ) {
        schemaChildren.put( schema.name, ImmutableList.of() );
        schemas.put( schema.name, schema );
    }


    @Override
    public void addTable( TableEntry table ) {
        String schema = table.schemaName;
        ArrayList<String> newTables = new ArrayList<>( schemaChildren.get( schema ) );
        newTables.add( table.name );
        schemaChildren.put( schema, ImmutableList.copyOf( newTables ) );
        tableChildren.put( schema + "." + table.name, ImmutableList.of() );
        tables.put( schema + "." + table.name, table );
    }


    @Override
    public void addColumn( ColumnEntry column ) {
        String schema = column.schemaName;
        String table = column.tableName;
        List<String> newColumns = new ArrayList<>( tableChildren.get( schema + "." + table ) );
        newColumns.add( column.name );
        tableChildren.put( schema + "." + table, ImmutableList.copyOf( newColumns ) );
        columns.put( schema + "." + table + "." + column.name, column );
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
        HTreeMap<String, SchemaEntry> schemasFile = this.file.hashMap( "schemas", Serializer.STRING, new GenericSerializer<SchemaEntry>() ).createOrOpen();
        ConcurrentMap<String, TableEntry> tablesFile = this.file.hashMap( "tables", Serializer.STRING, new GenericSerializer<TableEntry>() ).createOrOpen();
        ConcurrentMap<String, ColumnEntry> columnsFile = this.file.hashMap( "columns", Serializer.STRING, new GenericSerializer<ColumnEntry>() ).createOrOpen();

        schemasFile.clear();
        tablesFile.clear();
        columnsFile.clear();

        schemas.forEach( schemasFile::put );
        tables.forEach( tablesFile::put );
        columns.forEach( columnsFile::put );
    }

}
