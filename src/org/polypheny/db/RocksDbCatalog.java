package org.polypheny.db;


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

    private final List<ColumnFamilyHandle> columnFamilyHandles;
    private ColumnFamilyHandle schemas;
    private ColumnFamilyHandle schemaChildren;
    private ColumnFamilyHandle tables;
    private ColumnFamilyHandle tableChildren;
    private ColumnFamilyHandle columns;

    private RocksDB db;

    private final String db_path = "test";


    static {
        RocksDB.loadLibrary();
    }


    // TODO: renaming map identifiers
    public RocksDbCatalog() {

        // open DB with two column families
        final List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();

        final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

        columnFamilyDescriptors.add( new ColumnFamilyDescriptor( RocksDB.DEFAULT_COLUMN_FAMILY, new ColumnFamilyOptions() ) );

        columnFamilyDescriptors.add( new ColumnFamilyDescriptor( "schemas".getBytes(), new ColumnFamilyOptions() ) );
        columnFamilyDescriptors.add( new ColumnFamilyDescriptor( "schemaChildren".getBytes(), new ColumnFamilyOptions() ) );
        columnFamilyDescriptors.add( new ColumnFamilyDescriptor( "tables".getBytes(), new ColumnFamilyOptions() ) );
        columnFamilyDescriptors.add( new ColumnFamilyDescriptor( "tablesChildren".getBytes(), new ColumnFamilyOptions() ) );
        columnFamilyDescriptors.add( new ColumnFamilyDescriptor( "columns".getBytes(), new ColumnFamilyOptions() ) );

        this.columnFamilyHandles = columnFamilyHandles;
        final DBOptions options = new DBOptions();

        //needed for in-memory handling
        options.setMaxOpenFiles( -1 );
        options.setCreateIfMissing( true );
        options.setCreateMissingColumnFamilies( true );
        try {
            open( columnFamilyDescriptors, columnFamilyHandles, options );
        } catch ( RocksDBException e ) {
            e.printStackTrace();
        }
    }


    private void open( List<ColumnFamilyDescriptor> columnFamilyDescriptors, List<ColumnFamilyHandle> columnFamilyHandles, DBOptions options ) throws RocksDBException {
        this.db = RocksDB.open( options, db_path, columnFamilyDescriptors, columnFamilyHandles );

        this.schemas = columnFamilyHandles.get( 0 );
        this.schemaChildren = columnFamilyHandles.get( 1 );
        this.tables = columnFamilyHandles.get( 2 );
        this.tableChildren = columnFamilyHandles.get( 3 );
        this.columns = columnFamilyHandles.get( 4 );

    }


    @Override
    public void close() {
        for ( final ColumnFamilyHandle handle : columnFamilyHandles ) {
            handle.close();
        }
        this.db.close();
    }


    @Override
    public void addSchema( SchemaEntry schema ) {
        try {
            String name = schema.getName();
            db.put( this.schemaChildren, name.getBytes(), Serializer.serialize( new ArrayList<>() ) );
            db.put( this.schemas, name.getBytes(), schema.serialize() );
        } catch ( RocksDBException e ) {
            e.printStackTrace();
        }
    }


    @Override
    public void addTable( TableEntry table ) {
        String schema = table.getSchema();
        try {
            byte[] bytes = db.get( this.schemaChildren, schema.getBytes() );
            ArrayList<String> list = Serializer.deserialize( bytes );
            ArrayList<String> tables = new ArrayList<>( list );
            tables.add( table.getName() );
            db.put( this.schemaChildren, schema.getBytes(), Serializer.serialize( tables ) );
            db.put( this.tables, (schema + "." + table.getName()).getBytes(), Serializer.serialize( table ) );
            db.put( this.tableChildren, (schema + "." + table.getName()).getBytes(), Serializer.serialize( new ArrayList<>() ) );
        } catch ( RocksDBException e ) {
            e.printStackTrace();
        }

    }


    @Override
    public void addColumn( ColumnEntry column ) {
        String schema = column.getSchema();
        String table = column.getTable();
        try {
            byte[] bytes = db.get( this.tableChildren, (schema + "." + table).getBytes() );
            ArrayList<String> columns = new ArrayList<>( Serializer.deserialize( bytes ) );
            columns.add( column.getName() );
            db.put( this.tableChildren, (schema + "." + table).getBytes(), Serializer.serialize( columns ) );
            db.put( this.columns, (schema + "." + table + "." + column.getName()).getBytes(), Serializer.serialize( column ) );
        } catch ( RocksDBException e ) {
            e.printStackTrace();
        }
    }


    @Override
    public SchemaEntry getSchema( String schema ) {
        try {
            return Serializer.deserialize( db.get( this.schemas, schema.getBytes() ) );
        } catch ( RocksDBException e ) {
            e.printStackTrace();
        }
        return null;
    }


    @Override
    public TableEntry getTable( String schema, String table ) {
        try {
            byte[] bytes = db.get( this.tables, (schema + "." + table).getBytes() );
            return Serializer.deserialize( bytes );
        } catch ( RocksDBException e ) {
            e.printStackTrace();
        }
        return null;
    }


    @Override
    public ColumnEntry getColumn( String schema, String table, String column ) {
        try {
            byte[] bytes = db.get( this.columns, (schema + "." + table + "." + column).getBytes() );
            return Serializer.deserialize( bytes );
        } catch ( RocksDBException e ) {
            e.printStackTrace();
        }
        return null;
    }


    @Override
    public List<String> getSchemaNames() {
        RocksIterator iterator = this.db.newIterator( this.schemas );
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
        RocksIterator iterator = this.db.newIterator( this.tables );
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
        RocksIterator iterator = this.db.newIterator( this.columns );
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
        RocksIterator iterator = this.db.newIterator( this.schemas );
        iterator.seekToFirst();
        List<SchemaEntry> schemas = new ArrayList<>();
        while ( iterator.isValid() ) {
            schemas.add( Serializer.deserialize( iterator.value() ) );
            iterator.next();
        }
        return schemas;
    }


    @Override
    public List<TableEntry> getTables() {
        RocksIterator iterator = this.db.newIterator( this.tables );
        iterator.seekToFirst();
        List<TableEntry> tables = new ArrayList<>();
        while ( iterator.isValid() ) {
            tables.add( Serializer.deserialize( iterator.value() ) );
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
        RocksIterator iterator = this.db.newIterator( this.columns );
        iterator.seekToFirst();
        List<ColumnEntry> columns = new ArrayList<>();
        while ( iterator.isValid() ) {
            columns.add( Serializer.deserialize( iterator.value() ) );
            iterator.next();
        }
        return columns;
    }


    @Override
    public List<ColumnEntry> getColumns( String schema ) {
        return null;
    }


    @Override
    public List<ColumnEntry> getColumn( String schema, String table ) {
        return null;
    }


}
