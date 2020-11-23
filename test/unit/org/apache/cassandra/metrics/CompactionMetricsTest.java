/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.metrics;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.JMX;
import javax.management.ObjectInstance;
import javax.management.ObjectName;

import com.google.common.collect.Sets;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.DisallowedDirectories;
import org.apache.cassandra.db.compaction.ActiveCompactionsTracker;
import org.apache.cassandra.db.compaction.CompactionController;
import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.db.compaction.CompactionIterator;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.MockSchema;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CompactionMetricsTest extends CQLTester
{
    private static final CompactionManager compactionManager = CompactionManager.instance;

    @BeforeClass
    public static void setup(){
        try
        {
            startJMXServer();
            createMBeanServerConnection();
        }
        catch (Exception e)
        {
            e.printStackTrace();
            fail("Unable to create jmx connection.");
        }
    }

    @Test
    public void basicMetricsTest() throws Throwable {

            /* MBeans to test:
            org.apache.cassandra.metrics:type=Compaction,name=PendingTasks
            org.apache.cassandra.metrics:type=Compaction,name=PendingTasksByTableName
            org.apache.cassandra.metrics:type=Compaction,name=CompletedTasks
            org.apache.cassandra.metrics:type=Compaction,name=TotalCompactionsCompleted
            org.apache.cassandra.metrics:type=Compaction,name=BytesCompacted
            org.apache.cassandra.metrics:type=Compaction,name=CompactionsReduced
            org.apache.cassandra.metrics:type=Compaction,name=SSTablesDroppedFromCompaction
            org.apache.cassandra.metrics:type=Compaction,name=CompactionsAborted
            * */

            Predicate<String> findDomain = domainName -> domainName.equalsIgnoreCase("org.apache.cassandra.metrics");
            boolean match = Arrays.stream(jmxConnection.getDomains()).anyMatch(findDomain);
            assertTrue(match);

            try
            {
                ObjectName pendingTasks = new ObjectName("org.apache.cassandra.metrics:type=Compaction,name=PendingTasks");
                jmxConnection.getAttribute(pendingTasks, "Value");
                ObjectName pendingTasksByName = new ObjectName("org.apache.cassandra.metrics:type=Compaction,name=PendingTasksByTableName");
                jmxConnection.getAttribute(pendingTasksByName, "Value");
                ObjectName completedTasks = new ObjectName("org.apache.cassandra.metrics:type=Compaction,name=CompletedTasks");
                jmxConnection.getAttribute(completedTasks, "Value");
                ObjectName totalCompactionsCompleted = new ObjectName("org.apache.cassandra.metrics:type=Compaction,name=TotalCompactionsCompleted");
                jmxConnection.getAttribute(totalCompactionsCompleted, "Count");
                ObjectName bytesCompacted = new ObjectName("org.apache.cassandra.metrics:type=Compaction,name=BytesCompacted");
                jmxConnection.getAttribute(bytesCompacted, "Count");
                ObjectName compactionsReduced = new ObjectName("org.apache.cassandra.metrics:type=Compaction,name=CompactionsReduced");
                jmxConnection.getAttribute(compactionsReduced, "Count");
                ObjectName ssTablesDroppedFromCompaction = new ObjectName("org.apache.cassandra.metrics:type=Compaction,name=SSTablesDroppedFromCompaction");
                jmxConnection.getAttribute(ssTablesDroppedFromCompaction, "Count");
                ObjectName compactionsAborted = new ObjectName("org.apache.cassandra.metrics:type=Compaction,name=CompactionsAborted");
                jmxConnection.getAttribute(compactionsAborted, "Count");
            }
            catch(AttributeNotFoundException attributeNotFoundException){
                attributeNotFoundException.printStackTrace();
                fail("MBean not containing the expected attribute.");
            }

    }

    @Test
    public void testSimpleCompactionMetricsForCompletedTasks() throws Throwable
    {

        final long initialCompletedTasks = compactionManager.getMetrics().completedTasks.getValue();
        assertEquals(0L, compactionManager.getMetrics().totalCompactionsCompleted.getCount());
        assertEquals(0L, compactionManager.getMetrics().bytesCompacted.getCount());

        this.createTable("CREATE TABLE %s (pk int, ck int, a int, b int, PRIMARY KEY (pk, ck))");
        this.getCurrentColumnFamilyStore().disableAutoCompaction();
        for (int i = 0; i < 10; ++i)
        {
            this.execute("INSERT INTO %s (pk, ck, a, b) VALUES (" + i + ", 2, 3, 4)");
        }

        this.getCurrentColumnFamilyStore().forceBlockingFlush();
        this.getCurrentColumnFamilyStore().forceMajorCompaction();

        assertEquals(1L, compactionManager.getMetrics().totalCompactionsCompleted.getCount());
        assertTrue(compactionManager.getMetrics().bytesCompacted.getCount() > 0L);
        assertTrue(initialCompletedTasks < compactionManager.getMetrics().completedTasks.getValue());
    }

    @Test
    public void testCompactionMetricsForPendingTasks()
    {

    }

    @Test
    public void testCompactionMetricsForCompactionsAborted() throws Throwable
    {
        createTable("create table %s (id int, id2 int, t text, primary key (id, id2))");
        execute("insert into %s (id, id2) values (1, 1)");
        flush();

        for(File dir : getCurrentColumnFamilyStore().getDirectories().getCFDirectories()){
            DisallowedDirectories.maybeMarkUnwritable(dir);
        }

        try
        {
            getCurrentColumnFamilyStore().forceMajorCompaction();
        }
        catch (Exception e){
            logger.info("Exception will be thrown when compacting with unwritable directories.");
        }
        assertTrue(compactionManager.getMetrics().compactionsAborted.getCount() > 0);
    }

    @Test
    public void testCompactionMetricsForDroppedSSTables () throws Throwable
    {
        // assertTrue(compactionManager.getMetrics().compactionsReduced.getCount() > 0);
        // assertTrue(compactionManager.getMetrics().sstablesDropppedFromCompactions.getCount() > 0);
    }
}