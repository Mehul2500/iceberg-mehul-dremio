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
import org.apache.iceberg.*;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopTableOperations;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.hive.HiveTableOperations;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.projectnessie.model.ImmutableTableReference;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CatalogMigration {
    public static void migrationFunction(String identifiers, String sourceProperties, String targetProperties, Object sourceHadoopConfig, Object targetHadoopConfig) throws ClassNotFoundException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
            Map<String, String> sourceCatalog = splitToMap(sourceProperties, ",", "=");
            Map<String, String> targetCatalog = splitToMap(targetProperties, ",", "=");
            Map<String, String> tableIdentifiers = splitToMap(identifiers, ",", "=");
            List<TableIdentifier> listIdentifiers = new ArrayList<TableIdentifier>();
            for (Map.Entry<String, String> pair : tableIdentifiers.entrySet()) {
                TableIdentifier identifier = TableIdentifier.of(pair.getKey(), pair.getValue());
                listIdentifiers.add(identifier);
            }
            CatalogUtil.migrateTables(listIdentifiers, sourceCatalog, targetCatalog, sourceHadoopConfig, targetHadoopConfig);
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
