<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# Full-Text Search: SAI `LIKE` with `index_analyzer`

Server-side substring search over text columns (log lines, event messages, alarm text), designed
to combine with the time-series query pattern: restrict a partition and a clustering (time) range,
then filter by message content.

```sql
SELECT ts, msg FROM logs
 WHERE device = 'pump-01'
   AND ts >= '2026-07-31 00:00' AND ts < '2026-07-31 01:00'
   AND msg LIKE '%타임아웃%';
```

Korean works (CJK text is n-grammed like everything else), fragments may cross spaces
(`'%프 정%'`), and mid-word latin fragments match (`'%imeou%'` finds `"timeout"`).

## 1. Creating an analyzed index

```sql
CREATE TABLE logs (
    device text, ts timestamp, msg text,
    PRIMARY KEY (device, ts)
) WITH CLUSTERING ORDER BY (ts ASC);

-- substring-capable index: this is the preset that enables LIKE
CREATE INDEX logs_msg_idx ON logs(msg) USING 'sai'
  WITH OPTIONS = { 'index_analyzer': 'ngram' };
```

`index_analyzer` accepts a preset name or an Astra-compatible JSON definition:

| Preset | Tokenization | Enables `LIKE`? | Use for |
| --- | --- | --- | --- |
| **`ngram`** | character n-grams (2..3) over the whole value, spaces included, no case folding | **yes** | log/message substring search — the reason this feature exists |
| `standard` | Unicode words, lowercased | no | word-level term matching via `=`-style lookups |
| `cjk` | words + CJK bigrams (unigrams kept), lowercased | no | Astra-compatible CJK term matching |
| `keyword` | whole value, untouched | no | opting into the analyzer plumbing without tokenization |

JSON form (tokenizers: `keyword`, `standard`, `whitespace`, `ngram`; filters: `lowercase`,
`asciifolding`, `normalize`, `cjkwidth`, `cjkbigram`):

```sql
CREATE INDEX logs_msg_idx ON logs(msg) USING 'sai'
  WITH OPTIONS = { 'index_analyzer': '{
      "tokenizer" : {"name" : "ngram", "args" : {"minGramSize" : "2", "maxGramSize" : "3"}},
      "filters" : [{"name" : "lowercase"}]
  }' };
```

Adding `lowercase` makes `LIKE` case-insensitive (both the grams and the recheck fold case);
without it `LIKE` is case-sensitive, the standard SQL semantics.

`index_analyzer` cannot be combined with the legacy `case_sensitive`/`normalize`/`ascii` options,
is rejected on primary-key and collection columns, and there is no `ALTER INDEX` — changing the
analyzer means `DROP INDEX` + `CREATE INDEX` (full rebuild).

## 2. Querying

All four `LIKE` shapes work, with exact SQL semantics against the raw value:

```sql
-- substring (the primary form); mid-word fragments match
SELECT ts, msg FROM logs WHERE device='pump-01' AND ts >= ? AND ts < ? AND msg LIKE '%timeout%';

-- prefix / suffix / exact
... AND msg LIKE 'connection%';
... AND msg LIKE '%port 9042';
... AND msg LIKE 'connection refused';     -- whole-value match (like =, but analyzer-normalized)

-- multiple fragments AND together by repeating the restriction
... AND msg LIKE '%connection%' AND msg LIKE '%refused%';

-- '=' keeps exact whole-value semantics on an analyzed column
... AND msg = 'connection timeout on port 9042';
```

No `ALLOW FILTERING` is needed. Combining with the time-series functions works as usual:

```sql
SELECT time_bucket(5m, ts) AS bucket, count(*) AS errors
FROM logs
WHERE device = 'pump-01' AND ts >= ? AND ts < ? AND msg LIKE '%timeout%'
GROUP BY device, time_bucket(5m, ts);
```

## 3. How it works (and why it is exact)

**The index provides recall; a raw-value recheck provides precision.**

- Write path: the value is normalized (the analyzer's char-level filters), then an
  `NGramTokenizer(2,3)` slides over the whole character stream — spaces and punctuation
  included — and every gram becomes a term in the ordinary SAI inverted index. Values shorter
  than 2 characters are indexed as themselves so `=` keeps finding them.
- Query path: the `LIKE` fragment is normalized and cut into grams of a single size
  (`min(3, fragment length)`); each gram becomes an exact term lookup and the results are
  intersected. Because grams slide over the whole stream, every gram of a substring of a value
  is guaranteed to be indexed for that value — the index can produce false positives
  (grams present but scattered) but never false negatives.
- Post-filter: every candidate row is rechecked by applying the *complete original pattern* to
  the *raw column value* (`contains`/`startsWith`/`endsWith`/`equals`, after the analyzer's
  char-level normalization of both sides). Scattered-gram false positives die here. Every
  gram-expression carries the full pattern, so the recheck holds even when the engine truncates
  the intersection (`cassandra.sai.intersection_clause_limit`, default 2) or relaxes AND to OR
  during replica reconciliation.

`=` on an analyzed column is also served by gram intersection, but its recheck is raw byte
equality — exact whole-value semantics are preserved no matter how the index folded its grams.

## 4. Limits and costs

- **Minimum fragment length is the gram size (2).** `LIKE '%a%'` is rejected with an explicit
  error rather than returning silently wrong results.
- **Index size.** `ngram(2,3)` emits roughly 2 terms per character: budget the SAI index at
  a few times the raw size of the indexed column, and expect proportionally more write-path work.
  Terms per value are capped at 8192 (`cassandra.sai.max_analyzed_terms_per_cell`); values longer
  than ~4 KB get truncated grams (a warning is logged; fragments occurring only past the cap
  will not match that row).
- **`LIKE` on a non-analyzed SAI index still requires `ALLOW FILTERING`** — behaviour is
  unchanged unless you opt in with `index_analyzer: 'ngram'`.
- **Interior `%` is literal.** `LIKE '%a%b%'` searches for the literal string `a%b`
  (only leading/trailing `%` are wildcards — standard Cassandra `LIKE` parsing).
- **Word-level presets don't enable `LIKE`.** A word-tokenized index cannot guarantee substring
  recall, so accepting `LIKE` on it would silently miss rows; only `ngram` opens the gate.
- **Mixed-version clusters:** create analyzed indexes only after every node runs this version
  (older nodes reject the unknown option loudly and mark the index non-queryable).
- Guardrail `sai_string_term_size` applies per emitted term (not to the raw value), so long
  messages index correctly on both the memtable and flush paths.

## 5. Tests

- `org.apache.cassandra.index.sai.analyzer.LuceneTokenizingAnalyzerTest` — tokenization, presets,
  JSON parsing, the recall superset property.
- `org.apache.cassandra.index.sai.cql.AnalyzedLikeQueryTest` — end-to-end LIKE semantics across
  memtable/sstable/compacted states, false-positive elimination, korean, multi-fragment AND,
  `=` preservation, rejection cases.
- `org.apache.cassandra.distributed.test.sai.AnalyzedLikeDistributedTest` — 3-node cluster,
  RF=3, consistency levels, paging.
- `docker/integration-test.sh` — release-gate checks through a real node and cqlsh.
