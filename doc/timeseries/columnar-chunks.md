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

# 청크 포맷 v3 — 제거된 포맷

v3 컬럼 지향 청크 포맷은 **v4로 완전히 대체됐습니다**. 이중 읽기 경로는 없습니다 — 버전
바이트 3(v1·v2와 마찬가지로)을 만난 읽기 경로는 `UnsupportedChunkFormatException`을 던지고
이를 전파합니다(절대 건너뛰지 않음). v3 인코더/디코더와 그 전용 클래스들은 트리에서
삭제됐습니다.

현행 포맷 규격은 **[chunk-format-v4.md](chunk-format-v4.md)** 를 보십시오. v4가 v3에서 무엇을
유지하고 무엇을 폐기했는지는 그 문서의 §0, 제거된 포맷의 버전 바이트 처리는 §10에 있습니다.
