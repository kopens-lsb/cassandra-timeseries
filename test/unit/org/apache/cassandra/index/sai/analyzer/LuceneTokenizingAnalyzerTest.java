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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link LuceneTokenizingAnalyzer} and {@link AnalyzerOptions}: tokenization behaviour of the
 * {@code index_analyzer} presets (latin/korean n-grams, word tokens, CJK bigrams), the whole-value fallback for
 * short values, the Lucene TokenStream lifecycle across reuse, query-side gram generation, and option
 * parsing/validation.
 * <p>
 * The {@code ngram} preset uses NGramTokenizer(2, 3): at each character offset it emits the 2-gram then the
 * 3-gram, so the expected token lists below follow that order.
 */
public class LuceneTokenizingAnalyzerTest
{
    // ---------------------------------------------------------------- ngram preset (substring-capable)

    @Test
    public void latinNgrams() throws Exception
    {
        assertEquals(List.of("ti", "tim", "im", "ime", "me", "meo", "eo", "eou", "ou", "out", "ut"),
                     analyze("timeout", "ngram"));
    }

    @Test
    public void ngramsCrossSpaces() throws Exception
    {
        // grams span the space so fragments crossing word boundaries stay findable
        assertEquals(List.of("ab", "ab ", "b ", "b c", " c", " cd", "cd"), analyze("ab cd", "ngram"));
    }

    @Test
    public void koreanNgrams() throws Exception
    {
        assertEquals(List.of("펌프", "펌프 ", "프 ", "프 정", " 정", " 정지", "정지"), analyze("펌프 정지", "ngram"));
    }

    @Test
    public void mixedKoreanEnglishNgrams() throws Exception
    {
        assertEquals(List.of("펌프", "펌프p", "프p", "프pu", "pu", "pum", "um", "ump", "mp"),
                     analyze("펌프pump", "ngram"));
    }

    @Test
    public void ngramIsCaseSensitiveByDefault() throws Exception
    {
        // the ngram preset applies no case folding: LIKE keeps SQL case-sensitive semantics end to end
        assertEquals(List.of("Ti", "Tim", "im", "ime", "me"), analyze("Time", "ngram"));
    }

    @Test
    public void valueShorterThanGramFallsBackToWholeValue() throws Exception
    {
        // a value shorter than the minimum gram size must stay findable (whole-value fallback term)
        assertEquals(List.of("a"), analyze("a", "ngram"));
        assertEquals(List.of("펌"), analyze("펌", "ngram"));
    }

    @Test
    public void emptyValueEmitsNothing() throws Exception
    {
        assertEquals(List.of(), analyze("", "ngram"));
    }

    @Test
    public void whitespaceOnlyValueDoesNotThrow() throws Exception
    {
        assertEquals(List.of("  "), analyze("  ", "ngram"));
    }

    @Test
    public void wildcardCharIsLiteralInGrams() throws Exception
    {
        // '%' has no special meaning to the analyzer; LikePattern strips only leading/trailing wildcards
        assertEquals(List.of("a%", "a%b", "%b"), analyze("a%b", "ngram"));
    }

    // ---------------------------------------------------------------- standard / cjk / keyword presets

    @Test
    public void standardTokenizesWordsAndLowercases() throws Exception
    {
        assertEquals(List.of("connection", "timeout"), analyze("Connection TIMEOUT", "standard"));
    }

    @Test
    public void cjkEmitsHangulBigramsAndLatinWords() throws Exception
    {
        // CJKBigramFilter with outputUnigrams=true: unigrams interleaved with bigrams; latin words untouched
        assertEquals(List.of("펌", "펌프", "프", "프가", "가", "timeout"), analyze("펌프가 Timeout", "cjk"));
    }

    @Test
    public void cjkSingleHangulCharacterRemainsFindable() throws Exception
    {
        assertEquals(List.of("펌"), analyze("펌", "cjk"));
    }

    @Test
    public void keywordEmitsWholeValueUntouched() throws Exception
    {
        assertEquals(List.of("Connection TIMEOUT 펌프"), analyze("Connection TIMEOUT 펌프", "keyword"));
    }

    @Test
    public void onlyNgramIsSubstringCapable()
    {
        assertTrue(AnalyzerOptions.fromOptionValue("ngram").isSubstringCapable());
        assertFalse(AnalyzerOptions.fromOptionValue("standard").isSubstringCapable());
        assertFalse(AnalyzerOptions.fromOptionValue("cjk").isSubstringCapable());
        assertFalse(AnalyzerOptions.fromOptionValue("keyword").isSubstringCapable());
    }

    // ---------------------------------------------------------------- query-side gram generation

    @Test
    public void queryGramsUseSingleSize() throws Exception
    {
        // query side emits a single gram size g = min(maxGram, length) so every gram is guaranteed indexed
        assertEquals(List.of("tim", "ime", "meo", "eou", "out"), queryTerms("timeout", "ngram"));
        assertEquals(List.of("정지"), queryTerms("정지", "ngram"));
        assertEquals(List.of("ab"), queryTerms("ab", "ngram"));
    }

    @Test
    public void queryGramsCrossSpaces() throws Exception
    {
        assertEquals(List.of("프 정"), queryTerms("프 정", "ngram"));
    }

    @Test
    public void queryShorterThanMinimumFallsBackToWholeValue() throws Exception
    {
        // sub-minimum inputs still emit the whole value: EQ needs it to meet the write-side fallback term.
        // LIKE rejects such fragments before this point (minimumQueryLength check at the coordinator).
        assertEquals(List.of("a"), queryTerms("a", "ngram"));
    }

    // ---------------------------------------------------------------- lifecycle

    @Test
    public void reuseAcrossValues() throws Exception
    {
        AbstractAnalyzer analyzer = analyzerFor("ngram");
        assertEquals(List.of("ab", "abc", "bc"), drain(analyzer, "abc"));
        assertEquals(List.of("펌프"), drain(analyzer, "펌프"));
        assertEquals(List.of("xy"), drain(analyzer, "xy"));
    }

    @Test
    public void endThenResetIsSafe() throws Exception
    {
        AbstractAnalyzer analyzer = analyzerFor("ngram");
        analyzer.reset(utf8("abc"));
        assertTrue(analyzer.hasNext());
        analyzer.end();
        assertEquals(List.of("de"), drain(analyzer, "de"));
    }

    @Test
    public void nonStringTypeCannotBeAnalyzed()
    {
        assertThatThrownBy(() -> AbstractAnalyzer.fromOptions(SAITester.createIndexTermType(Int32Type.instance),
                                                              Map.of(AnalyzerOptions.INDEX_ANALYZER, "ngram")))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("cannot be analyzed");
    }

    @Test
    public void transformValueIsTrue()
    {
        assertTrue(analyzerFor("ngram").transformValue());
        assertTrue(analyzerFor("keyword").transformValue());
    }

    // ---------------------------------------------------------------- option parsing / validation

    @Test
    public void jsonConfigIsParsedAndApplied() throws Exception
    {
        // Astra-shaped JSON: explicit tokenizer with args and a filter chain
        String json = "{\"tokenizer\" : {\"name\" : \"ngram\", \"args\" : {\"minGramSize\" : \"3\", \"maxGramSize\" : \"3\"}}," +
                      " \"filters\" : [{\"name\" : \"lowercase\"}]}";
        assertEquals(List.of("tim", "ime", "meo", "eou", "out"), analyze("TimeOut", json));
    }

    @Test
    public void lowercaseFilterIsAppliedAsNormalizer() throws Exception
    {
        String json = "{\"tokenizer\" : {\"name\" : \"ngram\"}, \"filters\" : [{\"name\" : \"lowercase\"}]}";
        AnalyzerOptions options = AnalyzerOptions.fromOptionValue(json);
        // char-level filters are hoisted into the normalizer so the LIKE recheck can reproduce them exactly
        assertEquals("timeout", options.normalize("TimeOut"));
        assertEquals(List.of("ti", "tim", "im", "ime", "me"), analyze("TIME", json));
    }

    @Test
    public void jsonStandardWithCjkBigramFilter() throws Exception
    {
        String json = "{\"tokenizer\" : {\"name\" : \"standard\"}," +
                      " \"filters\" : [{\"name\" : \"lowercase\"}, {\"name\" : \"cjkbigram\"}]}";
        assertEquals(List.of("펌", "펌프", "프", "프가", "가", "stop"), analyze("펌프가 STOP", json));
    }

    @Test
    public void unknownPresetIsRejected()
    {
        assertThatThrownBy(() -> AnalyzerOptions.fromOptionValue("korean"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("korean")
        .hasMessageContaining("ngram");   // error must list the valid presets
    }

    @Test
    public void unknownTokenizerIsRejected()
    {
        assertThatThrownBy(() -> AnalyzerOptions.fromOptionValue("{\"tokenizer\" : {\"name\" : \"nori\"}}"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("nori");
    }

    @Test
    public void unknownFilterIsRejected()
    {
        assertThatThrownBy(() -> AnalyzerOptions.fromOptionValue(
            "{\"tokenizer\" : {\"name\" : \"standard\"}, \"filters\" : [{\"name\" : \"soundex\"}]}"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("soundex");
    }

    @Test
    public void unknownTokenizerArgIsRejected()
    {
        assertThatThrownBy(() -> AnalyzerOptions.fromOptionValue(
            "{\"tokenizer\" : {\"name\" : \"ngram\", \"args\" : {\"gramSize\" : \"3\"}}}"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("gramSize");
    }

    @Test
    public void malformedJsonIsRejected()
    {
        assertThatThrownBy(() -> AnalyzerOptions.fromOptionValue("{\"tokenizer\" :"))
        .isInstanceOf(InvalidRequestException.class);
    }

    @Test
    public void jsonWithoutTokenizerIsRejected()
    {
        assertThatThrownBy(() -> AnalyzerOptions.fromOptionValue("{\"filters\" : [{\"name\" : \"lowercase\"}]}"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("tokenizer");
    }

    @Test
    public void conflictingAnalyzerOptionsAreRejected()
    {
        assertThatThrownBy(() -> AbstractAnalyzer.fromOptions(SAITester.createIndexTermType(UTF8Type.instance),
                                                              Map.of(AnalyzerOptions.INDEX_ANALYZER, "ngram",
                                                                     NonTokenizingOptions.CASE_SENSITIVE, "false")))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining(AnalyzerOptions.INDEX_ANALYZER);
    }

    @Test
    public void minimumQueryLengthPerPreset()
    {
        assertEquals(2, AnalyzerOptions.fromOptionValue("ngram").minimumQueryLength());
        assertEquals(2, AnalyzerOptions.fromOptionValue("cjk").minimumQueryLength());
        assertEquals(1, AnalyzerOptions.fromOptionValue("standard").minimumQueryLength());
        assertEquals(1, AnalyzerOptions.fromOptionValue("keyword").minimumQueryLength());
    }

    @Test
    public void factoryIsRecognisedFromOptions() throws Exception
    {
        AbstractAnalyzer.AnalyzerFactory factory =
            AbstractAnalyzer.fromOptions(SAITester.createIndexTermType(UTF8Type.instance),
                                         Map.of(AnalyzerOptions.INDEX_ANALYZER, "ngram"));
        assertNotNull(factory);
        assertTrue(factory.isSubstringCapable());
        assertEquals(List.of("ab", "abc", "bc"), drain(factory.create(), "abc"));
        factory.close();
    }

    @Test
    public void analyzerOptionsAreExtractedForMetadata()
    {
        Map<String, String> extracted =
            AbstractAnalyzer.getAnalyzerOptions(Map.of(AnalyzerOptions.INDEX_ANALYZER, "ngram", "target", "v"));
        assertEquals(Map.of(AnalyzerOptions.INDEX_ANALYZER, "ngram"), extracted);
    }

    @Test
    public void toStringDescribesConfiguration()
    {
        String description = AnalyzerOptions.fromOptionValue("ngram").toString();
        assertTrue(description, description.contains("ngram"));
        assertFalse(description, description.contains("@"));
    }

    // ---------------------------------------------------------------- recall superset property

    @Test
    public void gramsOfSubstringAreAlwaysIndexed() throws Exception
    {
        // the property the whole LIKE design rests on: for any value V and substring P with |P| >= minGram,
        // every query gram of P is among the indexed grams of V
        String[] values = { "connection timeout on pump-01", "펌프 정지 알림 2026", "ab", "x y z", "MiXeD CaSe 한글" };
        for (String value : values)
        {
            List<String> indexed = analyze(value, "ngram");
            for (int from = 0; from < value.length() - 1; from++)
            {
                for (int len = 2; len <= Math.min(6, value.length() - from); len++)
                {
                    String fragment = value.substring(from, from + len);
                    for (String gram : queryTerms(fragment, "ngram"))
                        assertTrue("gram '" + gram + "' of fragment '" + fragment + "' not indexed for '" + value + '\'',
                                   indexed.contains(gram));
                }
            }
        }
    }

    // ---------------------------------------------------------------- helpers

    private static List<String> analyze(String input, String optionValue) throws Exception
    {
        return drain(analyzerFor(optionValue), input);
    }

    private static List<String> queryTerms(String input, String optionValue) throws Exception
    {
        AbstractAnalyzer.AnalyzerFactory factory = factoryFor(optionValue);
        return drain(factory.createQueryAnalyzer(), input);
    }

    private static AbstractAnalyzer analyzerFor(String optionValue)
    {
        return factoryFor(optionValue).create();
    }

    private static AbstractAnalyzer.AnalyzerFactory factoryFor(String optionValue)
    {
        AbstractAnalyzer.AnalyzerFactory factory =
            AbstractAnalyzer.fromOptions(SAITester.createIndexTermType(UTF8Type.instance),
                                         Map.of(AnalyzerOptions.INDEX_ANALYZER, optionValue));
        assertNotNull("expected a tokenizing analyzer factory", factory);
        return factory;
    }

    private static List<String> drain(AbstractAnalyzer analyzer, String input) throws Exception
    {
        List<String> tokens = new ArrayList<>();
        analyzer.reset(utf8(input));
        try
        {
            while (analyzer.hasNext())
                tokens.add(ByteBufferUtil.string(analyzer.next()));
        }
        finally
        {
            analyzer.end();
        }
        return tokens;
    }

    private static ByteBuffer utf8(String input)
    {
        return ByteBuffer.wrap(input.getBytes(StandardCharsets.UTF_8));
    }
}
