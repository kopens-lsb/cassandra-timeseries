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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.index.sai.utils.IndexTermType;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.NoSpamLogger;

/**
 * Tokenizing analyzer backed by a Lucene {@link Analyzer} chain built from {@link AnalyzerOptions}
 * ({@code index_analyzer} index option). Emits one term per Lucene token, after applying the char-level
 * normalization ({@link AnalyzerOptions#normalize(String)}) to the whole value first.
 * <p>
 * Values that produce no tokens at all (e.g. shorter than the minimum gram size) fall back to emitting the
 * whole normalized value as a single term, so they remain findable by {@code =} — without this a two-character
 * gram index would silently lose one-character values.
 * <p>
 * The number of terms emitted per value is capped by {@code cassandra.sai.max_analyzed_terms_per_cell}
 * (default 8192): an n-gram chain emits roughly two terms per character, so an unbounded value would flood the
 * index. Truncation is logged (no-spam) because it means fragments occurring only past the cap are not
 * findable for that row.
 */
public class LuceneTokenizingAnalyzer extends AbstractAnalyzer
{
    private static final Logger logger = LoggerFactory.getLogger(LuceneTokenizingAnalyzer.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 1, TimeUnit.MINUTES);

    private static final int MAX_TERMS_PER_CELL = CassandraRelevantProperties.SAI_MAX_ANALYZED_TERMS_PER_CELL.getInt();

    private final IndexTermType indexTermType;
    private final Analyzer luceneAnalyzer;
    private final AnalyzerOptions options;

    private TokenStream stream;
    private CharTermAttribute termAttribute;
    private String input;
    private boolean emitted;
    private int tokenCount;

    LuceneTokenizingAnalyzer(IndexTermType indexTermType, Analyzer luceneAnalyzer, AnalyzerOptions options)
    {
        this.indexTermType = indexTermType;
        this.luceneAnalyzer = luceneAnalyzer;
        this.options = options;
    }

    @Override
    public boolean transformValue()
    {
        return true;
    }

    @Override
    protected void resetInternal(ByteBuffer input)
    {
        closeStream();
        this.input = null;
        this.emitted = false;
        this.tokenCount = 0;

        if (!indexTermType.isString())
            return;

        try
        {
            String value = indexTermType.asString(input);
            this.input = value == null ? null : options.normalize(value);
        }
        catch (MarshalException e)
        {
            logger.error("Failed to deserialize value with {}", indexTermType, e);
        }
    }

    @Override
    public boolean hasNext()
    {
        next = null;
        nextLiteral = null;

        if (input == null || input.isEmpty())
            return false;

        try
        {
            if (stream == null)
            {
                stream = luceneAnalyzer.tokenStream("", input);
                termAttribute = stream.getAttribute(CharTermAttribute.class);
                stream.reset();
            }

            if (tokenCount >= MAX_TERMS_PER_CELL)
            {
                noSpamLogger.warn("index_analyzer emitted more than {} terms for a single value of length {}; " +
                                  "the remainder is not indexed and fragments occurring only past the cap will " +
                                  "not match this row (cassandra.sai.max_analyzed_terms_per_cell)",
                                  MAX_TERMS_PER_CELL, input.length());
                return false;
            }

            if (stream.incrementToken())
            {
                emitted = true;
                tokenCount++;
                nextLiteral = termAttribute.toString();
                next = indexTermType.fromString(nextLiteral);
                return true;
            }

            if (!emitted)
            {
                // whole-value fallback: values shorter than the minimum gram size emit no tokens but must
                // remain findable by '='
                emitted = true;
                nextLiteral = input;
                next = indexTermType.fromString(input);
                return true;
            }

            return false;
        }
        catch (IOException e)
        {
            logger.error("Failed to tokenize value for index_analyzer {}", options, e);
            return false;
        }
    }

    @Override
    public void end()
    {
        closeStream();
        input = null;
    }

    private void closeStream()
    {
        if (stream != null)
        {
            try
            {
                stream.end();
            }
            catch (IOException ignored)
            {
                // stream is being discarded either way
            }
            finally
            {
                try
                {
                    stream.close();
                }
                catch (IOException ignored)
                {
                    // stream is being discarded either way
                }
                stream = null;
                termAttribute = null;
            }
        }
    }

    @Override
    public String toString()
    {
        return options.toString();
    }

    /**
     * Query-side analyzer for substring-capable (n-gram) configurations: emits a sliding window of a single
     * gram size over the normalized input (see {@link AnalyzerOptions#queryGrams(String)}), so every emitted
     * term is guaranteed to exist in the index.
     */
    static class QueryGramAnalyzer extends AbstractAnalyzer
    {
        private final IndexTermType indexTermType;
        private final AnalyzerOptions options;

        private List<String> grams;
        private int position;

        QueryGramAnalyzer(IndexTermType indexTermType, AnalyzerOptions options)
        {
            this.indexTermType = indexTermType;
            this.options = options;
        }

        @Override
        public boolean transformValue()
        {
            return true;
        }

        @Override
        protected void resetInternal(ByteBuffer input)
        {
            grams = null;
            position = 0;

            if (!indexTermType.isString())
                return;

            try
            {
                String value = indexTermType.asString(input);
                if (value != null)
                    grams = options.queryGrams(options.normalize(value));
            }
            catch (MarshalException e)
            {
                logger.error("Failed to deserialize query value with {}", indexTermType, e);
            }
        }

        @Override
        public boolean hasNext()
        {
            next = null;
            nextLiteral = null;

            if (grams == null || position >= grams.size())
                return false;

            nextLiteral = grams.get(position++);
            next = indexTermType.fromString(nextLiteral);
            return true;
        }
    }
}
