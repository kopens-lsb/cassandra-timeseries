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

import java.text.Normalizer;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.cjk.CJKBigramFilter;
import org.apache.lucene.analysis.cjk.CJKWidthFilter;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter;
import org.apache.lucene.analysis.ngram.NGramTokenizer;
import org.apache.lucene.analysis.standard.StandardTokenizer;

import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.JsonUtils;
import org.apache.cassandra.utils.LocalizeString;

/**
 * Parsed and validated configuration of the {@code index_analyzer} SAI index option.
 * <p>
 * The option value is either a bare preset name ({@code keyword}, {@code standard}, {@code cjk}, {@code ngram})
 * or an Astra-shaped JSON object:
 * <pre>
 * {"tokenizer" : {"name" : "ngram", "args" : {"minGramSize" : "2", "maxGramSize" : "3"}},
 *  "filters" : [{"name" : "lowercase"}]}
 * </pre>
 * Character-level filters ({@code lowercase}, {@code asciifolding}, {@code normalize}) are hoisted out of the
 * Lucene chain and applied to the whole value <em>before</em> tokenization, via {@link #normalize(String)}.
 * This makes the transformation exactly reproducible on the raw value at post-filter time, which is what the
 * {@code LIKE} recheck relies on. Token-context filters ({@code cjkwidth}, {@code cjkbigram}) stay in the
 * Lucene chain.
 * <p>
 * Only the {@code ngram} tokenizer is substring-capable: its grams slide over the whole character stream
 * (spaces included), so every gram of a substring of a value is guaranteed to be an indexed gram of that
 * value. That recall property is what allows {@code LIKE '%fragment%'} to be index-accelerated; word-level
 * tokenizers cannot provide it and therefore do not enable {@code LIKE}.
 */
public class AnalyzerOptions
{
    public static final String INDEX_ANALYZER = "index_analyzer";

    static final String TOKENIZER_KEY = "tokenizer";
    static final String FILTERS_KEY = "filters";
    static final String NAME_KEY = "name";
    static final String ARGS_KEY = "args";
    static final String MIN_GRAM_SIZE = "minGramSize";
    static final String MAX_GRAM_SIZE = "maxGramSize";

    static final List<String> PRESETS = List.of("keyword", "standard", "cjk", "ngram");
    static final List<String> TOKENIZERS = List.of("keyword", "standard", "whitespace", "ngram");
    static final List<String> FILTERS = List.of("lowercase", "asciifolding", "normalize", "cjkwidth", "cjkbigram");

    static final int DEFAULT_MIN_GRAM = 2;
    static final int DEFAULT_MAX_GRAM = 3;
    static final int MAX_GRAM_BOUND = 8;

    private final String source;
    private final String tokenizer;
    private final int minGram;
    private final int maxGram;
    // char-level transforms, applied before tokenization and identically in the LIKE recheck
    private final boolean lowercase;
    private final boolean asciiFold;
    private final boolean nfcNormalize;
    // token-context filters that stay in the Lucene chain
    private final boolean cjkWidth;
    private final boolean cjkBigram;

    private AnalyzerOptions(String source, String tokenizer, int minGram, int maxGram,
                            boolean lowercase, boolean asciiFold, boolean nfcNormalize,
                            boolean cjkWidth, boolean cjkBigram)
    {
        this.source = source;
        this.tokenizer = tokenizer;
        this.minGram = minGram;
        this.maxGram = maxGram;
        this.lowercase = lowercase;
        this.asciiFold = asciiFold;
        this.nfcNormalize = nfcNormalize;
        this.cjkWidth = cjkWidth;
        this.cjkBigram = cjkBigram;
    }

    public static boolean hasOption(Map<String, String> options)
    {
        return options.containsKey(INDEX_ANALYZER);
    }

    public static AnalyzerOptions fromOptionValue(String value)
    {
        if (value == null || value.trim().isEmpty())
            throw new InvalidRequestException(INDEX_ANALYZER + " requires a value: one of " + PRESETS + " or a JSON analyzer definition");

        String trimmed = value.trim();
        if (trimmed.startsWith("{"))
            return fromJson(trimmed);

        switch (LocalizeString.toLowerCaseLocalized(trimmed, Locale.ROOT))
        {
            case "keyword":
                return new AnalyzerOptions(trimmed, "keyword", 0, 0, false, false, false, false, false);
            case "standard":
                return new AnalyzerOptions(trimmed, "standard", 0, 0, true, false, false, false, false);
            case "cjk":
                return new AnalyzerOptions(trimmed, "standard", 0, 0, true, false, false, true, true);
            case "ngram":
                return new AnalyzerOptions(trimmed, "ngram", DEFAULT_MIN_GRAM, DEFAULT_MAX_GRAM, false, false, false, false, false);
            default:
                throw new InvalidRequestException(String.format("Unknown %s '%s'. Supported presets: %s (or a JSON analyzer definition)",
                                                                INDEX_ANALYZER, trimmed, PRESETS));
        }
    }

    @SuppressWarnings("unchecked")
    private static AnalyzerOptions fromJson(String json)
    {
        Map<String, Object> config;
        try
        {
            config = JsonUtils.fromJsonMap(json);
        }
        catch (Exception e)
        {
            throw new InvalidRequestException(String.format("Malformed JSON in %s: %s", INDEX_ANALYZER, e.getMessage()));
        }

        for (String key : config.keySet())
            if (!key.equals(TOKENIZER_KEY) && !key.equals(FILTERS_KEY))
                throw new InvalidRequestException(String.format("Unsupported key '%s' in %s. Supported keys: [%s, %s]",
                                                                key, INDEX_ANALYZER, TOKENIZER_KEY, FILTERS_KEY));

        Object tokenizerSpec = config.get(TOKENIZER_KEY);
        if (!(tokenizerSpec instanceof Map))
            throw new InvalidRequestException(String.format("%s JSON requires a '%s' object, e.g. {\"%s\" : {\"%s\" : \"ngram\"}}",
                                                            INDEX_ANALYZER, TOKENIZER_KEY, TOKENIZER_KEY, NAME_KEY));

        Map<String, Object> tokenizerMap = (Map<String, Object>) tokenizerSpec;
        String tokenizer = stringField(tokenizerMap, NAME_KEY, TOKENIZER_KEY);
        if (!TOKENIZERS.contains(tokenizer))
            throw new InvalidRequestException(String.format("Unknown tokenizer '%s' in %s. Supported tokenizers: %s",
                                                            tokenizer, INDEX_ANALYZER, TOKENIZERS));

        int minGram = DEFAULT_MIN_GRAM;
        int maxGram = DEFAULT_MAX_GRAM;
        Object args = tokenizerMap.get(ARGS_KEY);
        if (args instanceof Map)
        {
            Map<String, Object> argsMap = (Map<String, Object>) args;
            for (String key : argsMap.keySet())
                if (!key.equals(MIN_GRAM_SIZE) && !key.equals(MAX_GRAM_SIZE))
                    throw new InvalidRequestException(String.format("Unknown tokenizer arg '%s' in %s. Supported args: [%s, %s]",
                                                                    key, INDEX_ANALYZER, MIN_GRAM_SIZE, MAX_GRAM_SIZE));
            minGram = intField(argsMap, MIN_GRAM_SIZE, minGram);
            maxGram = intField(argsMap, MAX_GRAM_SIZE, maxGram);
            if (minGram < 1 || maxGram > MAX_GRAM_BOUND || minGram > maxGram)
                throw new InvalidRequestException(String.format("%s/%s must satisfy 1 <= min <= max <= %d, got %d/%d",
                                                                MIN_GRAM_SIZE, MAX_GRAM_SIZE, MAX_GRAM_BOUND, minGram, maxGram));
        }

        boolean lowercase = false, asciiFold = false, nfcNormalize = false, cjkWidth = false, cjkBigram = false;
        Object filters = config.get(FILTERS_KEY);
        if (filters instanceof List)
        {
            for (Object filterSpec : (List<Object>) filters)
            {
                if (!(filterSpec instanceof Map))
                    throw new InvalidRequestException(String.format("Each entry of '%s' in %s must be an object with a '%s'",
                                                                    FILTERS_KEY, INDEX_ANALYZER, NAME_KEY));
                String filter = stringField((Map<String, Object>) filterSpec, NAME_KEY, FILTERS_KEY);
                switch (filter)
                {
                    case "lowercase":    lowercase = true;    break;
                    case "asciifolding": asciiFold = true;    break;
                    case "normalize":    nfcNormalize = true; break;
                    case "cjkwidth":     cjkWidth = true;     break;
                    case "cjkbigram":    cjkBigram = true;    break;
                    default:
                        throw new InvalidRequestException(String.format("Unknown filter '%s' in %s. Supported filters: %s",
                                                                        filter, INDEX_ANALYZER, FILTERS));
                }
            }
        }

        return new AnalyzerOptions(json, tokenizer, minGram, maxGram, lowercase, asciiFold, nfcNormalize, cjkWidth, cjkBigram);
    }

    private static String stringField(Map<String, Object> map, String key, String context)
    {
        Object value = map.get(key);
        if (!(value instanceof String) || ((String) value).isEmpty())
            throw new InvalidRequestException(String.format("'%s' requires a non-empty '%s' string in %s", context, key, INDEX_ANALYZER));
        return LocalizeString.toLowerCaseLocalized((String) value, Locale.ROOT);
    }

    private static int intField(Map<String, Object> map, String key, int defaultValue)
    {
        Object value = map.get(key);
        if (value == null)
            return defaultValue;
        try
        {
            return Integer.parseInt(value.toString());
        }
        catch (NumberFormatException e)
        {
            throw new InvalidRequestException(String.format("'%s' in %s must be an integer, got '%s'", key, INDEX_ANALYZER, value));
        }
    }

    /**
     * Applies the character-level transforms (lowercase, ascii folding, NFC) to a whole value. Used before
     * tokenization on the write path and, crucially, on the raw column value and pattern in the LIKE recheck —
     * both sides of the comparison must go through this exact function.
     */
    public String normalize(String value)
    {
        if (value == null)
            return null;
        String result = value;
        if (nfcNormalize && !Normalizer.isNormalized(result, Normalizer.Form.NFC))
            result = Normalizer.normalize(result, Normalizer.Form.NFC);
        if (lowercase)
            result = LocalizeString.toLowerCaseLocalized(result, Locale.ROOT);
        if (asciiFold)
        {
            char[] input = result.toCharArray();
            char[] folded = new char[input.length * 4];
            int length = ASCIIFoldingFilter.foldToASCII(input, 0, folded, 0, input.length);
            result = new String(folded, 0, length);
        }
        return result;
    }

    /** Only n-gram tokenization guarantees the recall superset property LIKE acceleration requires. */
    public boolean isSubstringCapable()
    {
        return tokenizer.equals("ngram");
    }

    /** The shortest query fragment the index can serve without false negatives. */
    public int minimumQueryLength()
    {
        if (tokenizer.equals("ngram"))
            return minGram;
        return cjkBigram ? 2 : 1;
    }

    int minGram()
    {
        return minGram;
    }

    /**
     * Query-side terms for an already-{@link #normalize(String) normalized} input: a sliding window of a
     * single gram size {@code g = min(maxGram, length)}, so every emitted gram is guaranteed to exist in the
     * index (which stores all sizes in {@code [minGram, maxGram]}). Inputs shorter than {@code minGram} fall
     * back to the whole value, matching the write-side whole-value fallback term — that keeps {@code =}
     * working on short values ({@code LIKE} rejects such fragments before this point).
     */
    public List<String> queryGrams(String normalized)
    {
        List<String> grams = new ArrayList<>();
        if (normalized == null || normalized.isEmpty())
            return grams;
        if (normalized.length() < minGram)
        {
            grams.add(normalized);
            return grams;
        }
        int size = Math.min(maxGram, normalized.length());
        for (int from = 0; from + size <= normalized.length(); from++)
            grams.add(normalized.substring(from, from + size));
        return grams;
    }

    /** Builds the write-side Lucene analyzer. The caller owns the returned instance (thread-safe, reusable). */
    public Analyzer buildLuceneAnalyzer()
    {
        return new Analyzer()
        {
            @Override
            protected TokenStreamComponents createComponents(String fieldName)
            {
                Tokenizer source;
                switch (tokenizer)
                {
                    case "ngram":      source = new NGramTokenizer(minGram, maxGram); break;
                    case "standard":   source = new StandardTokenizer();              break;
                    case "whitespace": source = new WhitespaceTokenizer();            break;
                    default:           source = new KeywordTokenizer();               break;
                }
                TokenStream stream = source;
                if (cjkWidth)
                    stream = new CJKWidthFilter(stream);
                if (cjkBigram)
                    stream = new CJKBigramFilter(stream, CJKBigramFilter.HAN | CJKBigramFilter.HIRAGANA |
                                                         CJKBigramFilter.KATAKANA | CJKBigramFilter.HANGUL, true);
                return new TokenStreamComponents(source, stream);
            }
        };
    }

    @Override
    public String toString()
    {
        return "AnalyzerOptions{" + source + '}';
    }
}
