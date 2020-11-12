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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
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

public class CompactionMetricsTest extends CQLTester
{
    private static final CompactionManager compactionManager = CompactionManager.instance;

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
        execute("insert into %s (id, id2) values (2, 2)");
        execute("insert into %s (id, id2) values (3, 3)");
        flush();
        int size = this.execute("select id from %s").size();

        for (int i = 0; i < 10; i++)
        {
            execute("delete from %s where id=? and id2=?", i, i);
        }
        flush();

        for (int i = 10; i < 20; i++)
        {
            execute("delete from %s where id=? and id2=?", i, i);
        }
        flush();

        for(File dir : getCurrentColumnFamilyStore().getDirectories().getCFDirectories()){
            DisallowedDirectories.maybeMarkUnwritable(dir);
        }

        try
        {
            getCurrentColumnFamilyStore().forceMajorCompaction();
        }
        catch (Exception e){
            logger.info("Exception Thrown when compacting with unwritable directories.");
        }
        assertTrue(compactionManager.getMetrics().compactionsAborted.getCount() > 0);
    }

    @Test
    public void testCompactionMetricsForFailedTasks () throws Throwable
    {
        this.createTable("CREATE TABLE %s (pk int, ck int, a int, b int, PRIMARY KEY (pk, ck))");
        this.getCurrentColumnFamilyStore().disableAutoCompaction();
        for (int i = 0; i < 10; ++i)
        {
            this.execute("INSERT INTO %s (pk, ck, a, b) VALUES (" + i + ", 2, 3, 4)");
        }

        Directories dr = getCurrentColumnFamilyStore().getDirectories();
        for(File dir : dr.getCFDirectories()){
            logger.info("{} write permission, yes? {}", dir.getAbsolutePath(), !DisallowedDirectories.isUnwritable(dir));
        }

        this.getCurrentColumnFamilyStore().forceBlockingFlush();
        this.getCurrentColumnFamilyStore().forceMajorCompaction();

        assertTrue(CompactionManager.instance.getMetrics().compactionsReduced.getCount() > 0);
        assertTrue(CompactionManager.instance.getMetrics().sstablesDropppedFromCompactions.getCount() > 0);
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

    private List<CompactionInfo.Holder> getActiveCompactionsForTable(ColumnFamilyStore cfs)
    {
        return compactionManager.active.getCompactions()
                                                .stream()
                                                .filter(holder -> holder.getCompactionInfo().getTable().orElse("unknown").equalsIgnoreCase(cfs.name))
                                                .collect(Collectors.toList());
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

        protected void dropExpiredSSTables(final Set<SSTableReader> fullyExpiredSSTables)
        {
            final Set<SSTableReader> nonExpiredSSTables = Sets.difference(txn.originals(), fullyExpiredSSTables);
            int nonExpiredSSTablesSize = nonExpiredSSTables.size();
            int sstablesRemoved = 0;

            while (nonExpiredSSTablesSize > 1)
            {
                compactionManager.incrementAborted();
                nonExpiredSSTablesSize--;
                sstablesRemoved++;
            }

            if (sstablesRemoved > 0)
            {
                compactionManager.incrementCompactionsReduced();
                compactionManager.incrementSstablesDropppedFromCompactions(sstablesRemoved);
            }
        }
    }
}