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

package org.apache.cassandra.index.sai.analyzer;

import org.apache.lucene.analysis.Analyzer;

import org.apache.cassandra.index.sai.utils.IndexTermType;

/**
 * Factory for {@link LuceneTokenizingAnalyzer} instances built from an {@code index_analyzer} option.
 * Holds the single shared (thread-safe) Lucene {@link Analyzer}; the per-use {@link AbstractAnalyzer}
 * wrappers it creates are cheap.
 */
public class LuceneAnalyzerFactory implements AbstractAnalyzer.AnalyzerFactory
{
    private final IndexTermType indexTermType;
    private final AnalyzerOptions options;
    private final Analyzer luceneAnalyzer;

    LuceneAnalyzerFactory(IndexTermType indexTermType, AnalyzerOptions options)
    {
        this.indexTermType = indexTermType;
        this.options = options;
        this.luceneAnalyzer = options.buildLuceneAnalyzer();
    }

    @Override
    public AbstractAnalyzer create()
    {
        return new LuceneTokenizingAnalyzer(indexTermType, luceneAnalyzer, options);
    }

    @Override
    public AbstractAnalyzer createQueryAnalyzer()
    {
        // for substring-capable (n-gram) configurations the query side emits a single gram size; for
        // word-level configurations the query value is analyzed exactly like an indexed value
        return options.isSubstringCapable() ? new LuceneTokenizingAnalyzer.QueryGramAnalyzer(indexTermType, options)
                                            : create();
    }

    @Override
    public boolean isTokenizing()
    {
        return true;
    }

    @Override
    public boolean isSubstringCapable()
    {
        return options.isSubstringCapable();
    }

    @Override
    public String normalize(String value)
    {
        return options.normalize(value);
    }

    @Override
    public int minimumQueryLength()
    {
        return options.minimumQueryLength();
    }

    @Override
    public void close()
    {
        luceneAnalyzer.close();
    }

    @Override
    public String toString()
    {
        return options.toString();
    }
}
