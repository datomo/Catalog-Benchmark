package org.polypheny.db;


import com.google.common.collect.Table;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.Filter;
import org.rocksdb.HashLinkedListMemTableConfig;
import org.rocksdb.HashSkipListMemTableConfig;
import org.rocksdb.HistogramData;
import org.rocksdb.HistogramType;
import org.rocksdb.Options;
import org.rocksdb.PlainTableConfig;
import org.rocksdb.RateLimiter;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.SkipListMemTableConfig;
import org.rocksdb.Statistics;
import org.rocksdb.TickerType;
import org.rocksdb.VectorMemTableConfig;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.rocksdb.util.SizeUnit;


public class RocksDbCatalog {

    static {
        RocksDB.loadLibrary();
    }


    public RocksDbCatalog() {
        final String db_path = "rockdb";
        final String db_path_not_found = db_path + "_not_found";

        System.out.println( "RocksDBSample" );
        try ( final Options options = new Options();
                final Filter bloomFilter = new BloomFilter( 10 );
                final ReadOptions readOptions = new ReadOptions()
                        .setFillCache( false );
                final Statistics stats = new Statistics();
                final RateLimiter rateLimiter = new RateLimiter( 10000000, 10000, 10 ) ) {

            try ( final RocksDB db = RocksDB.open( options, db_path_not_found ) ) {
                assert (false);
            } catch ( final RocksDBException e ) {
                System.out.format( "Caught the expected exception -- %s\n", e );
            }

            try {
                options.setCreateIfMissing( true )
                        .setStatistics( stats )
                        .setWriteBufferSize( 8 * SizeUnit.KB )
                        .setMaxWriteBufferNumber( 3 )
                        .setMaxBackgroundCompactions( 10 )
                        .setCompressionType( CompressionType.SNAPPY_COMPRESSION )
                        .setCompactionStyle( CompactionStyle.UNIVERSAL );
            } catch ( final IllegalArgumentException e ) {
                assert (false);
            }

            assert (options.createIfMissing() == true);
            assert (options.writeBufferSize() == 8 * SizeUnit.KB);
            assert (options.maxWriteBufferNumber() == 3);
            assert (options.maxBackgroundCompactions() == 10);
            assert (options.compressionType() == CompressionType.SNAPPY_COMPRESSION);
            assert (options.compactionStyle() == CompactionStyle.UNIVERSAL);

            assert (options.memTableFactoryName().equals( "SkipListFactory" ));
            options.setMemTableConfig(
                    new HashSkipListMemTableConfig()
                            .setHeight( 4 )
                            .setBranchingFactor( 4 )
                            .setBucketCount( 2000000 ) );
            assert (options.memTableFactoryName().equals( "HashSkipListRepFactory" ));

            options.setMemTableConfig(
                    new HashLinkedListMemTableConfig()
                            .setBucketCount( 100000 ) );
            assert (options.memTableFactoryName().equals( "HashLinkedListRepFactory" ));

            options.setMemTableConfig(
                    new VectorMemTableConfig().setReservedSize( 10000 ) );
            assert (options.memTableFactoryName().equals( "VectorRepFactory" ));

            options.setMemTableConfig( new SkipListMemTableConfig() );
            assert (options.memTableFactoryName().equals( "SkipListFactory" ));

            options.setTableFormatConfig( new PlainTableConfig() );
            // Plain-Table requires mmap read
            options.setAllowMmapReads( true );
            assert (options.tableFactoryName().equals( "PlainTable" ));

            options.setRateLimiter( rateLimiter );

            final BlockBasedTableConfig table_options = new BlockBasedTableConfig();
            table_options.setBlockCacheSize( 64 * SizeUnit.KB )
                    .setFilter( bloomFilter )
                    .setCacheNumShardBits( 6 )
                    .setBlockSizeDeviation( 5 )
                    .setBlockRestartInterval( 10 )
                    .setCacheIndexAndFilterBlocks( true )
                    .setHashIndexAllowCollision( false )
                    .setBlockCacheCompressedSize( 64 * SizeUnit.KB )
                    .setBlockCacheCompressedNumShardBits( 10 );

            assert (table_options.blockCacheSize() == 64 * SizeUnit.KB);
            assert (table_options.cacheNumShardBits() == 6);
            assert (table_options.blockSizeDeviation() == 5);
            assert (table_options.blockRestartInterval() == 10);
            assert (table_options.cacheIndexAndFilterBlocks() == true);
            assert (table_options.hashIndexAllowCollision() == false);
            assert (table_options.blockCacheCompressedSize() == 64 * SizeUnit.KB);
            assert (table_options.blockCacheCompressedNumShardBits() == 10);

            options.setTableFormatConfig( table_options );
            assert (options.tableFactoryName().equals( "BlockBasedTable" ));

            try ( final RocksDB db = RocksDB.open( options, db_path ) ) {
                db.put( "hello".getBytes(), "world".getBytes() );

                final byte[] value = db.get( "hello".getBytes() );
                assert ("world".equals( new String( value ) ));

                final String str = db.getProperty( "rocksdb.stats" );
                assert (str != null && !str.equals( "" ));
            } catch ( final RocksDBException e ) {
                System.out.format( "[ERROR] caught the unexpected exception -- %s\n", e );
                assert (false);
            }

            try ( final RocksDB db = RocksDB.open( options, db_path ) ) {
                db.put( "hello".getBytes(), "world".getBytes() );
                db.put( "test".getBytes(), new SchemaEntry("test").getBytes() );
                byte[] schema = db.get( "test".getBytes() );
                System.out.format(
                        new TableEntry( new String(schema) ).getName() );



            } catch ( final RocksDBException e ) {
                System.err.println( e );
            }
        }
    }

}
