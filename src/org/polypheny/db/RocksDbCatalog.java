package org.polypheny.db;


import java.util.ArrayList;
import java.util.List;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;


/**
 * Test implementation of a Rocks store, ignores a lot of possible errors etc.
 */
public class RocksDbCatalog implements DbCatalog {

    private final List<ColumnFamilyHandle> columnFamilyHandles;
    private ColumnFamilyHandle schema;
    private ColumnFamilyHandle schemaChildren;
    private ColumnFamilyHandle table;
    private ColumnFamilyHandle tableChildren;
    private ColumnFamilyHandle column;

    private RocksDB db;

    private final String db_path = "test";


    static {
        RocksDB.loadLibrary();
    }


    // TODO: renaming map identifiers
    public RocksDbCatalog() {
        final String db_path = "rockdb";

        // open DB with two column families
        final List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();

        final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

        columnFamilyDescriptors.add( new ColumnFamilyDescriptor( RocksDB.DEFAULT_COLUMN_FAMILY, new ColumnFamilyOptions() ) );

        columnFamilyDescriptors.add( new ColumnFamilyDescriptor( "schema".getBytes(), new ColumnFamilyOptions() ) );
        columnFamilyDescriptors.add( new ColumnFamilyDescriptor( "schemaChildren".getBytes(), new ColumnFamilyOptions() ) );
        columnFamilyDescriptors.add( new ColumnFamilyDescriptor( "tables".getBytes(), new ColumnFamilyOptions() ) );
        columnFamilyDescriptors.add( new ColumnFamilyDescriptor( "tablesChildren".getBytes(), new ColumnFamilyOptions() ) );
        columnFamilyDescriptors.add( new ColumnFamilyDescriptor( "column".getBytes(), new ColumnFamilyOptions() ) );

        this.columnFamilyHandles = columnFamilyHandles;
        final DBOptions options = new DBOptions();
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

        this.schema = columnFamilyHandles.get( 0 );
        this.schemaChildren = columnFamilyHandles.get( 1 );
        this.table = columnFamilyHandles.get( 2 );
        this.tableChildren = columnFamilyHandles.get( 3 );
        this.column = columnFamilyHandles.get( 4 );

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
            byte[] bytes = Serializer.toByteArray( new ArrayList<>() );
            db.put( this.schemaChildren, schema.getName().getBytes(), bytes );
            db.put( this.schema, schema.getBytes(), schema.getBytes() );
        } catch ( RocksDBException e ) {
            e.printStackTrace();
        }
    }


    @Override
    public void addTable( TableEntry table ) {
        String schema = table.getSchema();
        try {
            byte[] bytes = db.get( this.schemaChildren, schema.getBytes() );
            List<String> list = (ArrayList) Serializer.fromByteArray( bytes );
            List<String> tables = new ArrayList<>( list );
            tables.add( table.getName() );
            db.put( this.schemaChildren, schema.getBytes(), Serializer.toByteArray( tables ) );
            db.put( this.table, (schema + "." + table.getName()).getBytes(), table.getBytes() );
            db.put( this.tableChildren, (schema + "." + table.getName()).getBytes(), Serializer.toByteArray( new ArrayList<>() ) );
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
            List<String> columns = new ArrayList<>( (ArrayList) (Serializer.fromByteArray( bytes )) );
            columns.add( column.getName() );
            db.put( this.tableChildren, (schema + "." + table).getBytes(), Serializer.toByteArray( columns ) );
            db.put( this.column, (schema + "." + table + "." + column.getName()).getBytes(), column.getBytes() );
        } catch ( RocksDBException e ) {
            e.printStackTrace();
        }
    }


    @Override
    public SchemaEntry getSchema( String schema ) {
        try {
            return (SchemaEntry) Serializer.fromByteArray( db.get( this.schema, schema.getBytes() ) );
        } catch ( RocksDBException e ) {
            e.printStackTrace();
        }
        return null;
    }


    @Override
    public TableEntry getTable( String schema, String table ) {
        try {
            return (TableEntry) Serializer.fromByteArray( db.get( this.table, (this.schema + "." + this.table).getBytes() ) );
        } catch ( RocksDBException e ) {
            e.printStackTrace();
        }
        return null;
    }


    @Override
    public ColumnEntry getColumn( String schema, String table, String column ) {
        try {
            byte[] bytes = db.get( this.column, (schema + "." + table + "." + column).getBytes() );
            return (ColumnEntry) Serializer.fromByteArray( bytes );
        } catch ( RocksDBException e ) {
            e.printStackTrace();
        }
        return null;
    }


}
