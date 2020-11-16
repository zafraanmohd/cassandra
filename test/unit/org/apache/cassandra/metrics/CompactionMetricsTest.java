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

        final long initialCompletedTasks = compactionManager.getCompletedTasks();
        assertEquals(0L, compactionManager.getTotalCompactionsCompleted());
        assertEquals(0L, compactionManager.getTotalBytesCompacted());

        this.createTable("CREATE TABLE %s (pk int, ck int, a int, b int, PRIMARY KEY (pk, ck))");
        this.getCurrentColumnFamilyStore().disableAutoCompaction();
        for (int i = 0; i < 10; ++i)
        {
            this.execute("INSERT INTO %s (pk, ck, a, b) VALUES (" + i + ", 2, 3, 4)");
        }

        this.getCurrentColumnFamilyStore().forceBlockingFlush();
        this.getCurrentColumnFamilyStore().forceMajorCompaction();

        assertEquals(1L, compactionManager.getTotalCompactionsCompleted());
        assertTrue(compactionManager.getTotalBytesCompacted() > 0L);
        assertTrue(initialCompletedTasks < compactionManager.getCompletedTasks());
    }

    @Test
    public void testCompactionMetricsForPendingTasks()
    {
        final ColumnFamilyStore cfs = MockSchema.newCFS();
        final List<SSTableReader> sstables = this.createSSTables(cfs, 10, 0);
        final Set<SSTableReader> toMarkCompacting = new HashSet<>(sstables.subList(0, 3));
        final TestCompactionTask tct = new TestCompactionTask(cfs, toMarkCompacting);
        final long initialPendingTasks = compactionManager.getMetrics().pendingTasks.getValue();
        try
        {
            tct.start();
            assertTrue(compactionManager.getMetrics().pendingTasksByTableName.getValue().containsKey(cfs.keyspace.getName()));
            assertTrue(compactionManager.getMetrics().pendingTasks.getValue() > initialPendingTasks);
        }
        finally
        {
            tct.abort();
        }
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
        assertTrue(compactionManager.getMetrics().compactionsReduced.getCount() > 0);
        assertTrue(compactionManager.getMetrics().sstablesDropppedFromCompactions.getCount() > 0);
    }

    private List<SSTableReader> createSSTables(ColumnFamilyStore cfs, int count, int startGeneration)
    {
        List<SSTableReader> sstables = new ArrayList<>();
        for (int i = 0; i < count; i++)
        {
            long first = i * 10;
            long last = (i + 1) * 10 - 1;
            sstables.add(MockSchema.sstable(startGeneration + i, 0, true, first, last, cfs));
        }
        cfs.disableAutoCompaction();
        cfs.addSSTables(sstables);
        return sstables;
    }

    private static class TestCompactionTask
    {
        private final Set<SSTableReader> sstables;
        ActiveCompactionsTracker activeCompactions = compactionManager.active;
        OperationType compactionType = OperationType.COMPACTION;
        private final ColumnFamilyStore cfs;
        private LifecycleTransaction txn;
        private CompactionController controller;
        private CompactionIterator ci;
        private List<ISSTableScanner> scanners;

        public TestCompactionTask(ColumnFamilyStore cfs, Set<SSTableReader> sstables)
        {
            this.cfs = cfs;
            this.sstables = sstables;
        }

        public void start()
        {
            scanners = sstables.stream().map(SSTableReader::getScanner).collect(Collectors.toList());
            txn = cfs.getTracker().tryModify(sstables, OperationType.COMPACTION);
            assertNotNull(txn);
            controller = new CompactionController(cfs, sstables, Integer.MIN_VALUE);
            ci = new CompactionIterator(txn.opType(), scanners, controller, FBUtilities.nowInSeconds(), UUID.randomUUID());
            activeCompactions.beginCompaction(ci);
        }

        public void abort()
        {
            if (controller != null)
                controller.close();
            if (ci != null)
                ci.close();
            if (txn != null)
                txn.abort();
            if (scanners != null)
                scanners.forEach(ISSTableScanner::close);
            activeCompactions.finishCompaction(ci);
        }
    }
}