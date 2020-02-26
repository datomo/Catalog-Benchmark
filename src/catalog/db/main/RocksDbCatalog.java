package catalog.db.main;


import catalog.db.main.entity.ColumnEntry;
import catalog.db.main.entity.GenericSerializer;
import catalog.db.main.entity.SchemaEntry;
import catalog.db.main.entity.TableEntry;
import com.google.common.collect.ImmutableList;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;


/**
 * Test implementation of a Rocks store, ignores a lot of possible errors etc.
 */
public class RocksDbCatalog implements DbCatalog {

    private static List<ColumnFamilyHandle> columnFamilyHandles;
    private ColumnFamilyHandle schemas;
    private ColumnFamilyHandle schemaChildren;
    private ColumnFamilyHandle tables;
    private ColumnFamilyHandle tableChildren;
    private ColumnFamilyHandle columns;

    private static RocksDB db;

    private static final String FILE_PATH = "rocksDB";


    static {
        RocksDB.loadLibrary();
    }


    public RocksDbCatalog() {
        this( FILE_PATH );
    }


    public RocksDbCatalog( String path ) {

        final List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();

        columnFamilyDescriptors.add( new ColumnFamilyDescriptor( RocksDB.DEFAULT_COLUMN_FAMILY, new ColumnFamilyOptions() ) );

        columnFamilyDescriptors.add( new ColumnFamilyDescriptor( "schemas".getBytes(), new ColumnFamilyOptions() ) );
        columnFamilyDescriptors.add( new ColumnFamilyDescriptor( "schemaChildren".getBytes(), new ColumnFamilyOptions() ) );
        columnFamilyDescriptors.add( new ColumnFamilyDescriptor( "tables".getBytes(), new ColumnFamilyOptions() ) );
        columnFamilyDescriptors.add( new ColumnFamilyDescriptor( "tablesChildren".getBytes(), new ColumnFamilyOptions() ) );
        columnFamilyDescriptors.add( new ColumnFamilyDescriptor( "columns".getBytes(), new ColumnFamilyOptions() ) );

        final DBOptions options = new DBOptions();

        // needed for in-memory handling
        // options.setMaxOpenFiles( -1 );
        options.setCreateIfMissing( true );
        options.setCreateMissingColumnFamilies( true );
        try {
            open( columnFamilyDescriptors, path, options );
        } catch ( RocksDBException e ) {
            e.printStackTrace();
        }
    }


    private void open( List<ColumnFamilyDescriptor> columnFamilyDescriptors, String path, DBOptions options ) throws RocksDBException {

        if ( db != null ) {
            db.close();
        }

        columnFamilyHandles = new ArrayList<>();

        db = RocksDB.open( options, path, columnFamilyDescriptors, columnFamilyHandles );

        schemas = columnFamilyHandles.get( 0 );
        schemaChildren = columnFamilyHandles.get( 1 );
        tables = columnFamilyHandles.get( 2 );
        tableChildren = columnFamilyHandles.get( 3 );
        columns = columnFamilyHandles.get( 4 );

    }


    @Override
    public void close() {
        for ( final ColumnFamilyHandle handle : columnFamilyHandles ) {
            handle.close();
        }
        db.close();
    }


    @Override
    public void clear() {
        //TODO: check if persistent
    }


    @Override
    public void addSchema( SchemaEntry schema ) {
        try {
            String name = schema.name;
            db.put( this.schemaChildren, name.getBytes(), GenericSerializer.serialize( ImmutableList.of() ) );
            db.put( this.schemas, name.getBytes(), schema.serialize() );
        } catch ( RocksDBException e ) {
            e.printStackTrace();
        }
    }


    @Override
    public void addTable( TableEntry table ) {
        String schema = table.schemaName;
        try {
            byte[] bytes = db.get( this.schemaChildren, schema.getBytes() );
            ArrayList<String> tables = new ArrayList<>( GenericSerializer.deserialize( bytes ) );
            tables.add( table.name );
            db.put( this.schemaChildren, schema.getBytes(), GenericSerializer.serialize( ImmutableList.copyOf( tables ) ) );
            db.put( this.tables, (schema + "." + table.name).getBytes(), GenericSerializer.serialize( table ) );
            db.put( this.tableChildren, (schema + "." + table.name).getBytes(), GenericSerializer.serialize( ImmutableList.of() ) );
        } catch ( RocksDBException e ) {
            e.printStackTrace();
        }

    }


    @Override
    public void addColumn( ColumnEntry column ) {
        String schema = column.schemaName;
        String table = column.tableName;
        try {
            byte[] bytes = db.get( this.tableChildren, (schema + "." + table).getBytes() );
            ArrayList<String> columns = new ArrayList<>( GenericSerializer.deserialize( bytes ) );
            columns.add( column.name );
            db.put( this.tableChildren, (schema + "." + table).getBytes(), GenericSerializer.serialize( ImmutableList.copyOf( columns ) ) );
            db.put( this.columns, (schema + "." + table + "." + column.name).getBytes(), GenericSerializer.serialize( column ) );
        } catch ( RocksDBException e ) {
            e.printStackTrace();
        }
    }


    @Override
    public SchemaEntry getSchema( String schema ) {
        try {
            return GenericSerializer.deserialize( db.get( this.schemas, schema.getBytes() ) );
        } catch ( RocksDBException e ) {
            e.printStackTrace();
        }
        return null;
    }


    @Override
    public TableEntry getTable( String schema, String table ) {
        try {
            byte[] bytes = db.get( this.tables, (schema + "." + table).getBytes() );
            return GenericSerializer.deserialize( bytes );
        } catch ( RocksDBException e ) {
            e.printStackTrace();
        }
        return null;
    }


    @Override
    public ColumnEntry getColumn( String schema, String table, String column ) {
        try {
            byte[] bytes = db.get( this.columns, (schema + "." + table + "." + column).getBytes() );
            return GenericSerializer.deserialize( bytes );
        } catch ( RocksDBException e ) {
            e.printStackTrace();
        }
        return null;
    }


    @Override
    public List<String> getSchemaNames() {
        RocksIterator iterator = db.newIterator( this.schemas );
        iterator.seekToFirst();
        List<String> names = new ArrayList<>();
        while ( iterator.isValid() ) {
            names.add( new String( iterator.key(), StandardCharsets.UTF_8 ) );
            iterator.next();
        }
        return names;
    }


    @Override
    public List<String> getTableNames() {
        RocksIterator iterator = db.newIterator( this.tables );
        iterator.seekToFirst();
        List<String> names = new ArrayList<>();
        while ( iterator.isValid() ) {
            names.add( new String( iterator.key(), StandardCharsets.UTF_8 ) );
            iterator.next();
        }
        return names;
    }


    @Override
    public List<String> getColumnNames() {
        RocksIterator iterator = db.newIterator( this.columns );
        iterator.seekToFirst();
        List<String> names = new ArrayList<>();
        while ( iterator.isValid() ) {
            names.add( new String( iterator.key(), StandardCharsets.UTF_8 ) );
            iterator.next();
        }
        return names;
    }


    @Override
    public List<SchemaEntry> getSchemas() {
        RocksIterator iterator = db.newIterator( this.schemas );
        iterator.seekToFirst();
        List<SchemaEntry> schemas = new ArrayList<>();
        while ( iterator.isValid() ) {
            schemas.add( GenericSerializer.deserialize( iterator.value() ) );
            iterator.next();
        }
        return schemas;
    }


    @Override
    public List<TableEntry> getTables() {
        RocksIterator iterator = db.newIterator( this.tables );
        iterator.seekToFirst();
        List<TableEntry> tables = new ArrayList<>();
        while ( iterator.isValid() ) {
            tables.add( GenericSerializer.deserialize( iterator.value() ) );
            iterator.next();
        }
        return tables;
    }


    @Override
    public List<TableEntry> getTables( String schema ) {
        return null;
    }


    @Override
    public List<ColumnEntry> getColumns() {
        RocksIterator iterator = db.newIterator( this.columns );
        iterator.seekToFirst();
        List<ColumnEntry> columns = new ArrayList<>();
        while ( iterator.isValid() ) {
            columns.add( GenericSerializer.deserialize( iterator.value() ) );
            iterator.next();
        }
        return columns;
    }


    @Override
    public List<ColumnEntry> getColumns( String schema ) {
        return null;
    }


    @Override
    public ColumnEntry getColumn( String schema, String table ) {
        return null;
    }


    @Override
    public Long getColumn( Long schemaId, Long tableId ) {
        return null;
    }


    @Override
    public boolean isClosed() {
        // TODO: temporary fix
        return true;
    }


}
