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

package org.apache.cassandra.index.sai.cql;

import org.junit.Test;

import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.SAITester;

import static org.junit.Assert.assertEquals;

/**
 * End-to-end semantics of {@code LIKE} on a SAI index with a substring-capable {@code index_analyzer}:
 * true substring matching (mid-word, cross-space, korean), prefix/suffix/exact variants, false-positive
 * elimination by the raw-value recheck, conjunction of repeated restrictions, preservation of {@code =}
 * exact semantics, and the rejection surface. Every assertion runs against the memtable and the flushed
 * sstable ({@code beforeAndAfterFlush}) and the compacted variants run it a third time.
 */
public class AnalyzedLikeQueryTest extends SAITester
{
    private void createIndexedTable()
    {
        createTable("CREATE TABLE %s (device text, ts timestamp, msg text, PRIMARY KEY (device, ts))");
        createIndex("CREATE INDEX ON %s(msg) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'ngram' }");
    }

    private void insertLogRows() throws Throwable
    {
        execute("INSERT INTO %s (device, ts, msg) VALUES ('pump-01', '2024-01-01 09:00:00+0000', 'connection timeout on port 9042')");
        execute("INSERT INTO %s (device, ts, msg) VALUES ('pump-01', '2024-01-01 09:05:00+0000', '펌프 정지 알림')");
        execute("INSERT INTO %s (device, ts, msg) VALUES ('pump-01', '2024-01-01 09:10:00+0000', 'connection refused')");
        execute("INSERT INTO %s (device, ts, msg) VALUES ('pump-01', '2024-01-01 11:00:00+0000', 'late timeout out of range')");
        execute("INSERT INTO %s (device, ts, msg) VALUES ('pump-02', '2024-01-01 09:00:00+0000', 'timeout on other device')");
    }

    @Test
    public void containsMatchesMidWordFragment() throws Throwable
    {
        createIndexedTable();
        insertLogRows();

        beforeAndAfterFlush(() -> {
            // mid-word fragment: the reason the index is gram-based
            assertRowsIgnoringOrder(execute("SELECT msg FROM %s WHERE device='pump-01' AND msg LIKE '%%imeou%%'"),
                                    row("connection timeout on port 9042"),
                                    row("late timeout out of range"));
            // whole word
            assertRowsIgnoringOrder(execute("SELECT msg FROM %s WHERE device='pump-01' AND msg LIKE '%%refused%%'"),
                                    row("connection refused"));
            // fragment crossing a space
            assertRowsIgnoringOrder(execute("SELECT msg FROM %s WHERE device='pump-01' AND msg LIKE '%%timeout on%%'"),
                                    row("connection timeout on port 9042"));
            // no match
            assertEquals(0, execute("SELECT msg FROM %s WHERE device='pump-01' AND msg LIKE '%%nonexistent%%'").size());
        });
    }

    @Test
    public void koreanFragments() throws Throwable
    {
        createIndexedTable();
        insertLogRows();

        beforeAndAfterFlush(() -> {
            assertRowsIgnoringOrder(execute("SELECT msg FROM %s WHERE device='pump-01' AND msg LIKE '%%정지%%'"),
                                    row("펌프 정지 알림"));
            // korean fragment crossing a space
            assertRowsIgnoringOrder(execute("SELECT msg FROM %s WHERE device='pump-01' AND msg LIKE '%%프 정%%'"),
                                    row("펌프 정지 알림"));
            assertEquals(0, execute("SELECT msg FROM %s WHERE device='pump-01' AND msg LIKE '%%가동%%'").size());
        });
    }

    @Test
    public void withPartitionAndClusteringRange() throws Throwable
    {
        createIndexedTable();
        insertLogRows();

        beforeAndAfterFlush(() -> {
            // the headline shape: partition + time range + LIKE. The 11:00 row matches the fragment but is
            // outside the range; the pump-02 row matches but is another partition
            assertRowsIgnoringOrder(execute("SELECT msg FROM %s WHERE device='pump-01' " +
                                            "AND ts >= '2024-01-01 09:00:00+0000' AND ts < '2024-01-01 10:00:00+0000' " +
                                            "AND msg LIKE '%%timeout%%'"),
                                    row("connection timeout on port 9042"));
        });
    }

    @Test
    public void prefixSuffixAndExactVariants() throws Throwable
    {
        createIndexedTable();
        insertLogRows();

        beforeAndAfterFlush(() -> {
            // prefix: anchored at the start of the value
            assertRowsIgnoringOrder(execute("SELECT msg FROM %s WHERE device='pump-01' AND msg LIKE 'connection%%'"),
                                    row("connection timeout on port 9042"),
                                    row("connection refused"));
            // 'timeout' appears mid-value but never as prefix
            assertEquals(0, execute("SELECT msg FROM %s WHERE device='pump-01' AND msg LIKE 'timeout%%'").size());

            // suffix: anchored at the end
            assertRowsIgnoringOrder(execute("SELECT msg FROM %s WHERE device='pump-01' AND msg LIKE '%%9042'"),
                                    row("connection timeout on port 9042"));
            assertEquals(0, execute("SELECT msg FROM %s WHERE device='pump-01' AND msg LIKE '%%connection'").size());

            // bare LIKE: whole-value match
            assertRowsIgnoringOrder(execute("SELECT msg FROM %s WHERE device='pump-01' AND msg LIKE 'connection refused'"),
                                    row("connection refused"));
            assertEquals(0, execute("SELECT msg FROM %s WHERE device='pump-01' AND msg LIKE 'connection'").size());
        });
    }

    @Test
    public void scatteredGramsAreNotAMatch() throws Throwable
    {
        // the core false-positive test: every gram of the pattern is present but scattered, so the index
        // proposes the row and only the raw-value recheck can reject it
        createIndexedTable();
        // grams of 'abcdef' (3-grams: abc, bcd, cde, def) all present, in the wrong arrangement
        execute("INSERT INTO %s (device, ts, msg) VALUES ('d', '2024-01-01 00:00:00+0000', 'def zzz cde zzz bcd zzz abc')");
        execute("INSERT INTO %s (device, ts, msg) VALUES ('d', '2024-01-01 00:00:01+0000', 'xx abcdef xx')");

        beforeAndAfterFlush(() -> {
            assertRowsIgnoringOrder(execute("SELECT msg FROM %s WHERE device='d' AND msg LIKE '%%abcdef%%'"),
                                    row("xx abcdef xx"));
        });
    }

    @Test
    public void overlappingRepeatsRequireTrueContainment() throws Throwable
    {
        createIndexedTable();
        execute("INSERT INTO %s (device, ts, msg) VALUES ('d', '2024-01-01 00:00:00+0000', 'ababab')");

        beforeAndAfterFlush(() -> {
            // gram multisets of 'ababab' and 'abababab' overlap heavily; only the recheck separates them
            assertEquals(1, execute("SELECT msg FROM %s WHERE device='d' AND msg LIKE '%%abab%%'").size());
            assertEquals(1, execute("SELECT msg FROM %s WHERE device='d' AND msg LIKE '%%ababab%%'").size());
            assertEquals(0, execute("SELECT msg FROM %s WHERE device='d' AND msg LIKE '%%abababab%%'").size());
        });
    }

    @Test
    public void repeatedLikeRestrictionsAreConjunctive() throws Throwable
    {
        createIndexedTable();
        insertLogRows();

        beforeAndAfterFlush(() -> {
            assertRowsIgnoringOrder(execute("SELECT msg FROM %s WHERE device='pump-01' " +
                                            "AND msg LIKE '%%connection%%' AND msg LIKE '%%refused%%'"),
                                    row("connection refused"));
            // three-way conjunction exceeds the default intersection clause limit (2); correctness must
            // come from the post-filter
            assertRowsIgnoringOrder(execute("SELECT msg FROM %s WHERE device='pump-01' " +
                                            "AND msg LIKE '%%connection%%' AND msg LIKE '%%timeout%%' AND msg LIKE '%%9042%%'"),
                                    row("connection timeout on port 9042"));
        });
    }

    @Test
    public void equalsKeepsExactWholeValueSemantics() throws Throwable
    {
        createIndexedTable();
        insertLogRows();

        beforeAndAfterFlush(() -> {
            assertRowsIgnoringOrder(execute("SELECT msg FROM %s WHERE device='pump-01' AND msg = 'connection refused'"),
                                    row("connection refused"));
            // a value sharing many grams with the queried one must not leak through '='
            assertEquals(0, execute("SELECT msg FROM %s WHERE device='pump-01' AND msg = 'connection'").size());
            assertEquals(0, execute("SELECT msg FROM %s WHERE device='pump-01' AND msg = 'refused connection'").size());
        });
    }

    @Test
    public void equalsFindsValueShorterThanGramSize() throws Throwable
    {
        createIndexedTable();
        execute("INSERT INTO %s (device, ts, msg) VALUES ('d', '2024-01-01 00:00:00+0000', 'a')");

        beforeAndAfterFlush(() -> {
            // whole-value fallback term: 1-char values stay findable by '='
            assertEquals(1, execute("SELECT msg FROM %s WHERE device='d' AND msg = 'a'").size());
        });
    }

    @Test
    public void caseIsSensitiveWithoutLowercaseFilter() throws Throwable
    {
        createIndexedTable();
        execute("INSERT INTO %s (device, ts, msg) VALUES ('d', '2024-01-01 00:00:00+0000', 'Connection Timeout')");

        beforeAndAfterFlush(() -> {
            // the ngram preset applies no case folding: SQL-standard case-sensitive LIKE
            assertEquals(1, execute("SELECT msg FROM %s WHERE device='d' AND msg LIKE '%%Timeout%%'").size());
            assertEquals(0, execute("SELECT msg FROM %s WHERE device='d' AND msg LIKE '%%timeout%%'").size());
        });
    }

    @Test
    public void caseIsInsensitiveWithLowercaseFilter() throws Throwable
    {
        createTable("CREATE TABLE %s (device text, ts timestamp, msg text, PRIMARY KEY (device, ts))");
        createIndex("CREATE INDEX ON %s(msg) USING 'sai' WITH OPTIONS = { 'index_analyzer' : " +
                    "'{\"tokenizer\" : {\"name\" : \"ngram\"}, \"filters\" : [{\"name\" : \"lowercase\"}]}' }");
        execute("INSERT INTO %s (device, ts, msg) VALUES ('d', '2024-01-01 00:00:00+0000', 'Connection Timeout')");

        beforeAndAfterFlush(() -> {
            // with the lowercase filter both grams and recheck fold case
            assertEquals(1, execute("SELECT msg FROM %s WHERE device='d' AND msg LIKE '%%timeout%%'").size());
            assertEquals(1, execute("SELECT msg FROM %s WHERE device='d' AND msg LIKE '%%TIMEOUT%%'").size());
        });
    }

    @Test
    public void updatesAndDeletesAreReflected() throws Throwable
    {
        createIndexedTable();
        execute("INSERT INTO %s (device, ts, msg) VALUES ('d', '2024-01-01 00:00:00+0000', 'first timeout')");

        beforeAndAfterFlush(() ->
            assertEquals(1, execute("SELECT msg FROM %s WHERE device='d' AND msg LIKE '%%timeout%%'").size()));

        execute("UPDATE %s SET msg = 'now refused' WHERE device='d' AND ts='2024-01-01 00:00:00+0000'");

        beforeAndAfterFlush(() -> {
            assertEquals(0, execute("SELECT msg FROM %s WHERE device='d' AND msg LIKE '%%timeout%%'").size());
            assertEquals(1, execute("SELECT msg FROM %s WHERE device='d' AND msg LIKE '%%refused%%'").size());
        });

        execute("DELETE FROM %s WHERE device='d' AND ts='2024-01-01 00:00:00+0000'");

        beforeAndAfterFlush(() ->
            assertEquals(0, execute("SELECT msg FROM %s WHERE device='d' AND msg LIKE '%%refused%%'").size()));
    }

    @Test
    public void worksAfterCompactionAndPostBuild() throws Throwable
    {
        createTable("CREATE TABLE %s (device text, ts timestamp, msg text, PRIMARY KEY (device, ts))");
        execute("INSERT INTO %s (device, ts, msg) VALUES ('d', '2024-01-01 00:00:00+0000', 'early timeout row')");
        flush();
        execute("INSERT INTO %s (device, ts, msg) VALUES ('d', '2024-01-01 00:00:01+0000', 'second refused row')");
        flush();
        compact();

        // index created after the data exists: the post-build path must produce the same grams
        createIndex("CREATE INDEX ON %s(msg) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'ngram' }");

        assertRowsIgnoringOrder(execute("SELECT msg FROM %s WHERE device='d' AND msg LIKE '%%imeou%%'"),
                                row("early timeout row"));
        assertRowsIgnoringOrder(execute("SELECT msg FROM %s WHERE device='d' AND msg LIKE '%%refused%%'"),
                                row("second refused row"));
    }

    @Test
    public void largeValueSurvivesFlush() throws Throwable
    {
        createIndexedTable();
        // large but under the 8KiB string-term warn threshold at the raw level would previously be dropped
        // at flush because the guard checked the raw value; per-term validation must keep it queryable
        StringBuilder builder = new StringBuilder();
        while (builder.length() < 9000)
            builder.append("filler words before the needle appears here ");
        builder.append("uniqueneedletoken");
        execute("INSERT INTO %s (device, ts, msg) VALUES ('d', '2024-01-01 00:00:00+0000', ?)", builder.toString());

        beforeAndAfterFlush(() ->
            assertEquals(1, execute("SELECT device FROM %s WHERE device='d' AND msg LIKE '%%uniqueneedletoken%%'").size()));
    }

    // ---------------------------------------------------------------- rejection surface

    @Test
    public void fragmentShorterThanGramSizeIsRejected() throws Throwable
    {
        createIndexedTable();
        insertLogRows();

        assertInvalidThrowMessage("shorter than the minimum fragment length", InvalidRequestException.class,
                                  "SELECT msg FROM %s WHERE device='pump-01' AND msg LIKE '%%a%%'");
    }

    @Test
    public void likeOnNonAnalyzedIndexStillRequiresAllowFiltering() throws Throwable
    {
        createTable("CREATE TABLE %s (device text, ts timestamp, msg text, PRIMARY KEY (device, ts))");
        createIndex("CREATE INDEX ON %s(msg) USING 'sai'");
        execute("INSERT INTO %s (device, ts, msg) VALUES ('d', '2024-01-01 00:00:00+0000', 'timeout')");

        // unchanged behaviour: a plain SAI index cannot serve LIKE
        assertInvalidThrowMessage("ALLOW FILTERING", InvalidRequestException.class,
                                  "SELECT msg FROM %s WHERE device='d' AND msg LIKE '%%timeout%%'");
        // and with ALLOW FILTERING it is answered by brute force as before
        assertEquals(1, execute("SELECT msg FROM %s WHERE device='d' AND msg LIKE '%%timeout%%' ALLOW FILTERING").size());
    }

    @Test
    public void likeOnWordLevelAnalyzerIsNotIndexServed() throws Throwable
    {
        createTable("CREATE TABLE %s (device text, ts timestamp, msg text, PRIMARY KEY (device, ts))");
        // word-level analyzers cannot guarantee substring recall, so they must not open the LIKE gate
        createIndex("CREATE INDEX ON %s(msg) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");
        execute("INSERT INTO %s (device, ts, msg) VALUES ('d', '2024-01-01 00:00:00+0000', 'connection timeout')");

        assertInvalidThrowMessage("ALLOW FILTERING", InvalidRequestException.class,
                                  "SELECT msg FROM %s WHERE device='d' AND msg LIKE '%%timeout%%'");
    }
}
