/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.nessie;

import org.apache.hadoop.conf.Configuration;
import org.apache.http.util.TextUtils;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hive.HiveMetastoreTest;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.rules.TemporaryFolder;

import java.util.HashMap;
import java.util.Map;

import static org.apache.iceberg.types.Types.NestedField.required;

public class TestCatalogMigration extends TestNessieCatalog {
    protected static final String DB_NAME = "hivedb";
    static final String TABLE_NAME = "tbl";
    static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(DB_NAME, TABLE_NAME);
    static final TableIdentifier TABLE_IDENTIFIER1 = TableIdentifier.of("hadoopdb", TABLE_NAME);
    static final Schema schema = new Schema(Types.StructType.of(
            required(1, "id", Types.LongType.get())).fields());

    public TemporaryFolder temp1 = new TemporaryFolder();
    @Test
    public void testMigrationFunctionHive() throws Exception {
        HiveMetastoreTest.startMetastore();
        HiveMetastoreTest.catalog.createTable(TABLE_IDENTIFIER, schema);
        final Table icebergTable = HiveMetastoreTest.catalog.loadTable(TABLE_IDENTIFIER);
        String hiveUri = HiveMetastoreTest.catalog.getConf().get("hive.metastore.uris");
        String hiveWarehouse = HiveMetastoreTest.catalog.getConf().get("hive.metastore.warehouse.dir");
        Configuration hiveHadoopConf = HiveMetastoreTest.catalog.getConf();
//        String nessieUri = getUri();
//        NessieCatalog nessieCatalog = new NessieCatalog();
        String nessieUri = "http://localhost:19120/api/v1";
        String nessieWarehouse = "file:///var/folders/gv/5l6s3ykj2y91rzy37wp4bb9r0000gp/T/junit6171848636417203264/";
        String sourceImpl = "org.apache.iceberg.hive.HiveCatalog";
        String targetImpl = "org.apache.iceberg.nessie.NessieCatalog";
        String nessie = "warehouse=" + nessieWarehouse + ",ref=main,uri=" + nessieUri + ",catalogImpl=" + targetImpl + ",catalogName=nessie";
        Map<String, String> options = splitToMap(nessie, ",", "=");
        Configuration nessieHadoopConfig = new Configuration();
        Catalog catalog2 = CatalogUtil.loadCatalog("org.apache.iceberg.nessie.NessieCatalog", "nessie", options, nessieHadoopConfig);

        String hive = "warehouse=" + hiveWarehouse + ",uri=" + hiveUri + ",catalogImpl=" + sourceImpl + ",catalogName=hive";
        String identifiers = "hivedb=tbl";
        String branchName = "main";
        CatalogMigration migrator = new CatalogMigration();
        migrator.migrationFunction(identifiers, hive, nessie, hiveHadoopConf, nessieHadoopConfig);
        Assertions.assertThat(catalog2.loadTable(TABLE_IDENTIFIER)).isNotNull();
//        Assertions.assertThat(catalog2.dropTable(TABLE_IDENTIFIER)).isTrue();
    }

    @Test
    public void testMigrationFunctionHadoop() throws Exception {
        String warehouseLocation = "/var/folders/gv/5l6s3ykj2y91rzy37wp4bb9r0000gp/T/junit15726067253240743854/junit6006928396359110919";
        HadoopCatalog catalog1 = new HadoopCatalog();
        catalog1.setConf(new Configuration());
        Configuration hadoopConf = catalog1.getConf();
        catalog1.initialize("hadoop", ImmutableMap.of(CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation));
        catalog1.createTable(TABLE_IDENTIFIER1, schema);
        String nessieUri = getUri();
        String nessieWarehouse = catalog.defaultWarehouseLocation(TABLE_IDENTIFIER1);
        String sourceImpl = "org.apache.iceberg.hadoop.HadoopCatalog";
        String targetImpl = "org.apache.iceberg.nessie.NessieCatalog";
        String nessie = "warehouse=" + nessieWarehouse + ",ref=main,uri=" + nessieUri + ",catalogImpl=" + targetImpl + ",catalogName=nessie";
        String hadoop = "warehouse=" + warehouseLocation + ",catalogImpl=" + sourceImpl + ",catalogName=hadoop";
        String identifiers = "hadoopdb=tbl";
        Configuration nessieConf = catalog.getConf();
        CatalogMigration migrator = new CatalogMigration();
        migrator.migrationFunction(identifiers, hadoop, nessie, hadoopConf, nessieConf);
        Assertions.assertThat(catalog.loadTable(TABLE_IDENTIFIER1)).isNotNull();
        Assertions.assertThat(catalog1.dropTable(TABLE_IDENTIFIER1)).isTrue();
    }

    public static Map<String, String> splitToMap(String source, String entriesSeparator, String keyValueSeparator) {
        Map<String, String> map = new HashMap<String, String>();
        String[] entries = source.split(entriesSeparator);
        for (String entry : entries) {
            if (!TextUtils.isEmpty(entry) && entry.contains(keyValueSeparator)) {
                String[] keyValue = entry.split(keyValueSeparator);
                map.put(keyValue[0], keyValue[1]);
            }
        }
        return map;
    }
}
