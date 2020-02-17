package org.polypheny.db;// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

import org.rocksdb.*;

import java.util.ArrayList;
import java.util.List;

public class RocksDBColumnFamilySample {
    static {
        RocksDB.loadLibrary();
    }

    public static void main(final String[] args) throws RocksDBException {

        final String db_path = "test";

        System.out.println("RocksDBColumnFamilySample");

        // open DB with two column families
        final List<ColumnFamilyDescriptor> columnFamilyDescriptors =
                new ArrayList<>();
        // have to open default column family
        columnFamilyDescriptors.add(new ColumnFamilyDescriptor( RocksDB.DEFAULT_COLUMN_FAMILY, new ColumnFamilyOptions()));
        // open the new one, too
        //columnFamilyDescriptors.add(new ColumnFamilyDescriptor( "new_cf".getBytes(), new ColumnFamilyOptions()));
        columnFamilyDescriptors.add(new ColumnFamilyDescriptor( "tables".getBytes(), new ColumnFamilyOptions()));
        final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
        final DBOptions options = new DBOptions();
        options.setCreateMissingColumnFamilies( true );
        options.setCreateIfMissing( true );

        try( final RocksDB db = RocksDB.open(options, db_path, columnFamilyDescriptors, columnFamilyHandles)) {
            assert(db != null);

            try {
                // put and get from non-default column family
                db.put(columnFamilyHandles.get(0), new WriteOptions(),
                        "key".getBytes(), "value".getBytes());

                // atomic write
                try (final WriteBatch wb = new WriteBatch()) {
                    wb.put(columnFamilyHandles.get(0), "key2".getBytes(),
                            "value2".getBytes());
                    wb.put(columnFamilyHandles.get(1), "key3".getBytes(),
                            "value3".getBytes());
                    wb.remove(columnFamilyHandles.get(0), "key".getBytes());
                    db.write(new WriteOptions(), wb);
                }

                // drop column family
                db.dropColumnFamily(columnFamilyHandles.get(1));
            } finally {
                for (final ColumnFamilyHandle handle : columnFamilyHandles) {
                    handle.close();
                }
            }
        }
    }
}