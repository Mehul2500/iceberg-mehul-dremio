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
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.aws.AwsClientFactories;
import org.apache.iceberg.aws.AwsClientFactory;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.LocationUtil;
import org.apache.iceberg.util.LockManagers;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.http.HttpClientBuilder;
import org.projectnessie.jaxrs.ext.NessieUri;
import org.projectnessie.model.Branch;
import software.amazon.awssdk.services.glue.GlueClient;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.apache.iceberg.types.Types.NestedField.required;





public class TestGlueCatalogMigration extends TestNessieCatalog {
    protected static final String DB_NAME = "gluedb";
    static final String TABLE_NAME = "tbl";
    static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(DB_NAME, TABLE_NAME);
    static final Schema schema = new Schema(Types.StructType.of(
            required(1, "id", Types.LongType.get())).fields());
    static GlueCatalog glueCatalog;
    public static String testBucketName() {
        return System.getenv("AWS_TEST_BUCKET");
    }
    static final String testBucketName = testBucketName();
    static final String catalogName = "glue";
    public static String getRandomName() {
        return UUID.randomUUID().toString().replace("-", "");
    }
    static final String testPathPrefix = getRandomName();
    static final AwsClientFactory clientFactory = AwsClientFactories.defaultFactory();
    static final GlueClient glue = clientFactory.glue();

    @Test
    public void testMigrationFunctionGlue() {
        String testBucketPath = "s3://" + testBucketName + "/" + testPathPrefix;
        S3FileIO fileIO = new S3FileIO(clientFactory::s3);
        glueCatalog = new GlueCatalog();
        glueCatalog.initialize(catalogName, testBucketPath, new AwsProperties(), glue,
                LockManagers.defaultLockManager(), fileIO);
        String namespace = getRandomName();
        String tableName = getRandomName();
        glueCatalog.createNamespace(Namespace.of(namespace));
        TableIdentifier identifier = TableIdentifier.of(namespace, tableName);
        glueCatalog.createTable(identifier, schema);
        Table table = glueCatalog.loadTable(identifier);
//        String nessieUri = getUri();
//        String nessieWarehouse = catalog.defaultWarehouseLocation(identifier);
        String sourceImpl = "org.apache.iceberg.aws.glue.GlueCatalog";
        String targetImpl = "org.apache.iceberg.nessie.NessieCatalog";
        String nessie = "warehouse=file:///var/folders/gv/5l6s3ykj2y91rzy37wp4bb9r0000gp/T/junit2296007327427100623/,ref=main,uri=http://localhost:19120/api/v1" + ",catalogImpl=" + targetImpl + ",catalogName=nessie";
        Configuration hadoopConfig = new Configuration();
        Map<String, String> options = splitToMap(nessie, ",", "=");
        Catalog nessieCatalog = CatalogUtil.loadCatalog("org.apache.iceberg.nessie.NessieCatalog", "nessie", options, hadoopConfig);
//        nessieCatalog.createTable(TABLE_IDENTIFIER, schema);
        String glueWarehouse = LocationUtil.stripTrailingSlash(testBucketPath);
        String glue = "warehouse=" + glueWarehouse + "/" + namespace + ".db/" + tableName + ",catalogImpl=" + sourceImpl + ",catalogName=glue";
        String identifiers = namespace + "=" + tableName;
        CatalogMigration migrator = new CatalogMigration();
//        migrator.migrationFunction(identifiers, glue, nessie);
        Assertions.assertThat(nessieCatalog.loadTable(identifier)).isNotNull();
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

//    public void beforeEach(@NessieUri URI nessieUri) throws IOException {
//        this.uri = nessieUri.toString();
//        this.api = HttpClientBuilder.builder().withUri(this.uri).build(NessieApiV1.class);
//
//        resetData();
//
//        try {
//            api.createReference().reference(Branch.of("main", null)).create();
//        } catch (Exception e) {
//            // ignore, already created. Can't run this in BeforeAll as quarkus hasn't disabled auth
//        }
//
//        hadoopConfig = new Configuration();
//        catalog = initNessieCatalog("main");
//    }
}
