# DATA_NOTES — 矿业三源聚合 RAG 数据说明

> 项目：矿业新闻 + 关键矿产政策 + 商品价格 三源聚合管线（24h 交付）
> 本文档描述统一数据 Schema、`doc_id` 主键规则、各数据源去重策略及存储约定。

---

## 1. 数据源范围

| source_type | 数据源 | 采集方式 | 目标数量（近30天） |
|---|---|---|---|
| `news` | mining.com、S&P Global Mining | RSS 发现链接 → 全文爬取 → 结构化抽取 | ≥ 200 条 |
| `policy` | 中国稀土集团官网、澳洲 DISR Critical Minerals Strategy | 站点遍历 + HTML 不规则结构解析 | ≥ 200 条 |
| `price` | LME（铜/锌/镍）、SHFE（锂）、上海钢联（铁矿石） | 官方接口/页面，处理登录墙与频控 | ≥ 200 条 |

合计 ≥ 600 条 Document，切片后全量入向量库。

---

## 2. 统一数据 Schema

数据分为两层：**原始文档 Document**（去重后的权威存储层）与 **向量切片 Chunk**（检索层）。
一个 Document 切成 1..N 个 Chunk；Chunk 通过 `doc_id` 回链到 Document。

### 2.1 Document（原始文档）

```jsonc
{
  "doc_id":        "news-3a7f1c92b0e4",      // 主键，见 §4
  "source_type":   "news",                    // 枚举: news | policy | price
  "source":        "mining_com",              // 具体数据源标识，见 §2.3
  "title":         "Rio Tinto boosts ...",    // 清洗后标题，非空
  "url":           "https://www.mining.com/…",// 原始/规范化链接；price 类可为接口标识符
  "content":       "正文纯文本…",              // 清洗后正文（HTML 已去标签/去模板噪音）
  "summary":       "…",                        // 可选：RSS 摘要或抽取的导语
  "published_at":  "2026-09-18T08:30:00+00:00",// 原文发布时间（UTC ISO8601）
  "crawled_at":    "2026-10-15T02:11:43+00:00",// 采集时间（UTC ISO8601）
  "language":      "en",                       // zh | en
  "content_hash":  "sha256:…",                 // 归一化正文的 SHA-256，用于近似去重
  "metadata":      { … },                       // 分源扩展字段，见 §2.2
  "dedup_status":  "kept",                      // 枚举: kept | duplicate
  "duplicate_of":  null                          // dedup_status=duplicate 时指向保留记录的 doc_id
}
```

> `dedup_status` 与 `duplicate_of` 拆为两个字段（不在枚举值内嵌 `:<doc_id>` 格式），保证 SQLite 可直接建索引过滤。
> `published_at` 统一语义为**业务事件时间**：news 取原文发布时间；policy 取发文日期（当日 00:00 UTC）；price 取交易日（00:00 UTC）。“近30天/近7天”时间窗过滤一律以该字段为准。

#### 2.2 metadata 分源扩展字段

- **news**

```jsonc
"metadata": {
  "site_domain":   "mining.com",
  "author":        "…",
  "tags":          ["copper", "rio tinto"],
  "word_count":    860,
  "fulltext_ok":   true          // 全文爬取是否成功；false 时 content 回退为 RSS 摘要
}
```

- **policy**

```jsonc
"metadata": {
  "issuer":        "中国稀土集团",            // 发布机构
  "doc_number":    "中稀发〔2026〕12号",      // 公文编号，缺失置 null
  "publish_date":  "2026-09-30",              // 发文日期（正文页标注）
  "policy_type":   "strategy | notice | plan",// 政策类型
  "region":        "CN | AU",
  "crawl_page_no": 3                           // 列表页序号，便于回溯
}
```

- **price**（一条 Document = 一个品种 × 一个交易日）

```jsonc
"metadata": {
  "exchange":      "LME | SHFE | Mysteel",
  "symbol":        "CU | ZN | NI | LC | FE",   // 品种: 铜/锌/镍/碳酸锂/铁矿石
  "trade_date":    "2026-10-14",
  "price_type":    "settlement | close | spot",// 口径
  "currency":      "USD | CNY",
  "values": {                                  // 结构化价格快照（缺失字段为 null）
    "open": 9800.5, "high": 9910.0, "low": 9750.0,
    "close": 9875.0, "settlement": 9872.5,
    "volume": 12345, "change_pct": 0.31
  },
  "unit":         "$/t | 元/吨 | 元/手"
}
```

> price 类的 `content` 由结构化字段模板化生成自然语言摘要（如 "2026-10-14 LME 铜结算价 9872.5 美元/吨，涨 0.31%"），保证既能被向量检索命中，又保留精确数值字段供答案引用。

#### 2.3 source 枚举（数据源稳定标识）

| source 值 | 含义 |
|---|---|
| `mining_com` | mining.com 新闻 |
| `sp_global_mining` | S&P Global Mining 新闻 |
| `cnrare_policy` | 中国稀土集团官网政策 |
| `au_disr_policy` | 澳洲 DISR Critical Minerals Strategy |
| `lme_price` | LME 铜/锌/镍 |
| `shfe_price` | SHFE 碳酸锂 |
| `mysteel_price` | 上海钢联铁矿石 |

### 2.4 Chunk（向量切片）

```jsonc
{
  "chunk_id":     "news-3a7f1c92b0e4_003",     // 主键: {doc_id}_{chunk_index:03d}，序号补零
  "doc_id":       "news-3a7f1c92b0e4",         // 外键 → Document
  "chunk_index":  3,                            // 切片序号，0 起
  "text":         "…",                          // 切片正文，送 embedding
  "token_count":  412,
  "metadata": {                                 // 冗余 Document 关键字段快照，检索免回表
    "source_type":  "news",
    "source":       "mining_com",
    "title":        "Rio Tinto boosts ...",
    "url":          "https://www.mining.com/…",
    "published_at": "2026-09-18T08:30:00+00:00",
    "region":       null,                       // policy 冗余: CN | AU
    "tags":         ["copper", "rio tinto"],    // news/policy 冗余: 受控词表标签
    "trade_date":   null,                       // price 冗余
    "symbol":       null                        // price 冗余: CU | ZN | NI | LC | FE
  }
}
```

约定：
1. **`vector` 不作为 Chunk 存储字段**。JSONL（`data/chunks/`）只保存 text + metadata；embedding 向量单一来源存于 Chroma（`data/vector_db/`），避免文件膨胀与双写不一致。Schema 校验时 vector 视为运行时字段。
2. Chunk metadata 必须冗余 `source_type/published_at/region/tags/symbol` 等过滤字段：自然语言查询（如“近7天澳洲锂出口政策有何变化？”）由 serve 层解析出 `source_type=policy + region=AU + 时间窗` 的 where 过滤条件，直接在 Chroma metadata 上执行，同时保证检索结果无需回表即可返回 title/url 引用。
3. `region`、`symbol`、`tags` 使用受控枚举/词表（采集清洗阶段归一化，如 lithium/锂 → `lithium`），供查询解析器做同义词映射。

---

## 3. 字段约定与约束

| 字段 | 类型 | 必填 | 约束 |
|---|---|---|---|
| doc_id | str | ✓ | 主键，全局唯一，不可变 |
| source_type | enum | ✓ | news / policy / price |
| source | enum | ✓ | §2.3 枚举值 |
| title | str | ✓ | 清洗去空白后非空；price 由模板生成 |
| url | str | ✓ | 唯一链接或接口标识符（如 `lme://CU/2026-10-14/settlement`）；SQLite 建 UNIQUE 索引 |
| content | str | ✓ | ≥ 50 字符（news/policy），模板摘要（price） |
| published_at | datetime | ✓ | UTC ISO8601，**业务事件时间**：news 取原文发布时间；policy 取发文日期；price 取交易日 00:00 UTC |
| crawled_at | datetime | ✓ | 采集时刻 |
| language | enum | ✓ | zh / en |
| content_hash | str | ✓ | SHA-256 over §5.1 归一化正文 |
| metadata | object | ✓ | 分源字段，允许 null 值，禁止随意新增顶层字段 |
| duplicate_of | str | – | 判重记录指向保留 doc_id；kept 时为 null |

---

## 4. doc_id 主键生成规则

统一格式：`{source_type}-{12位小写十六进制哈希}`，哈希取 `sha1(稳定键)` 前 12 位。

| 源类型 | 稳定键（stable key） | 说明 |
|---|---|---|
| news | `normalize_url(url)` | URL 规范化：小写域名、去 query 中的跟踪参数（utm_* 等）、去 fragment、去尾部 `/`。RSS GUID 与详情页 URL 指向同一页面 → 同一 doc_id，天然合并摘要/全文两次写入 |
| policy | `normalize_url(url)` | 同上；详情页 URL 是政策文件唯一位置 |
| price | `f"{source}|{symbol}|{trade_date}|{price_type}"` | 业务复合键。同一交易日同一口径只有一条记录；接口重试、重复采集幂等 |

**upsert 语义**：管线全程以 `doc_id` 作主键做 UPSERT——
- news/policy：同 doc_id 再次采集时，若 `content_hash` 变化则更新 content 并重新切片，否则跳过；
- price：同 doc_id 重复出现时 **latest-wins**（保留最新 `crawled_at` 的数据，纠正盘中/收盘修订值）。

`chunk_id = {doc_id}_{chunk_index:03d}`（序号补零，避免 `_10` 与 `_2` 字典序错乱及前缀误匹配），重建索引时按 `doc_id` 前缀先删旧片再插新片，保证不残留孤儿 chunk。

---

## 5. 去重策略（分源）

### 5.1 公共归一化（所有源，去重前执行）

1. HTML → 纯文本（去标签、脚本、导航/版权等模板噪音）；
2. Unicode NFKC 归一、连续空白折叠为一个空格；
3. 英文小写化（中文不转），全角标点转半角；
4. 计算 `content_hash = sha256(归一化正文)`，先做 **L0 全文级精确去重**（同 hash 直接判重）。

去重顺序：L0 精确 hash → L1 键级 → L2 内容近似。所有被判定重复的记录在 `data/deduped/` 保留痕迹（`dedup_status=duplicate` + `duplicate_of=<doc_id>`），只把 `kept` 记录入库，保证数量可审计。

### 5.2 news（矿业新闻）

| 层 | 策略 | 针对场景 |
|---|---|---|
| L1 URL 级 | doc_id（规范化 URL 哈希）精确去重 | RSS 已收录后又全文爬取；同一文章出现在 RSS 多条目中 |
| L2 标题级 | 标题归一化（去标点/数字外的装饰符）后完全相等，且发布时间差 ≤ 48h → 判重 | mining.com 与 S&P Global 互相转载同一通稿 |
| L3 内容级 | SimHash（64bit）指纹，海明距离 ≤ 3 判近重复；保留 `crawled_at` 更早、正文更完整（word_count 更大）的一条 | 换标题转载、摘要扩写为全文 |

跨源判重仅在两个 news 子源之间做；与 policy/price 不互相判重。

### 5.3 policy（关键矿产政策）

| 层 | 策略 | 针对场景 |
|---|---|---|
| L1 URL 级 | doc_id 精确去重 | 官网同一文件挂在多个栏目列表页（HTML 结构不规则导致重复发现） |
| L2 公文编号 | `metadata.doc_number` 非空时，`(issuer, doc_number)` 唯一；再次出现判重，优先保留正文更完整者 | "XX〔2026〕N号" 文件被转发/镜像 |
| L3 标题+日期 | `(归一化标题, publish_date)` 相等判重 | doc_number 缺失的新闻稿/公告类页面 |
| L4 内容级 | SimHash 海明距离 ≤ 2（比 news 更严格阈值） | 政策摘要页与全文页；避免误杀"同系列不同文件" |

中澳两站点分属不同 `source`，语言与结构差异大，不做跨站点文本近似判重，只做 L1–L3 各自站点内去重。

### 5.4 price（价格数据）

价格数据是**结构化时序记录**，不做文本近似去重，只做业务主键去重：

| 层 | 策略 | 说明 |
|---|---|---|
| L1 业务复合键 | `(source, symbol, trade_date, price_type)` 即 doc_id，UPSERT + latest-wins | 频控重试、断点续爬、接口分页重叠产生的重复直接幂等合并 |
| L2 口径隔离 | 不同 `price_type`（settlement/close/spot）视为不同记录，不互相判重 | 防止把收盘价误当结算价删掉 |
| L3 异常过滤 | 非去重但配套：`trade_date` 超出近 30 天窗口、核心价格字段全为 null 的记录丢弃并计入 `data/raw/…/rejects.jsonl` | 保证入库存量满足"近30天≥200条"口径 |

---

## 6. 存储与目录约定

| 阶段 | 位置 | 格式 |
|---|---|---|
| 原始采集 | `data/raw/{news,policy,price}/*.jsonl` | 每行一条源内原始记录（含未清洗 HTML） |
| 清洗后 | `data/cleaned/*.jsonl` | Document 草稿（未去重） |
| 去重后 | `data/deduped/documents.jsonl` + `sqlite://data/deduped/documents.db` | 最终 Document，`doc_id` 主键 |
| 切片 | `data/chunks/chunks.jsonl` | Chunk 记录 |
| 向量库 | `data/vector_db/`（Chroma 持久化） | collection: `mining_rag`，存 embedding + chunk metadata |

`data/` 整体加入 `.gitignore`；向量库可全量由 `pipeline/run_all.py` 重建。

---

## 7. 切片（Chunking）策略摘要

| source_type | 策略 | 参数 |
|---|---|---|
| news | 段落聚合滑窗 | 目标 512 tokens，overlap 64，不切断句子 |
| policy | 按章节/条款边界切分，标题继承 | 512–1024 tokens，超长条款滑窗 overlap 128 |
| price | **不切分**：每条 Document 整条一个 chunk（模板摘要本身短） | chunk_index=0 |

每个 chunk 的 `text` 前拼接 `"【{title} | {published_at或trade_date}】\n"` 作为上下文头部，提升检索命中与答案引用准确性。

---

## 8. /query 接口返回与数据字段的对应

`serve/app.py` 的 `POST /query` 响应中，每条引用来源返回：
`doc_id, chunk_id, title, url, source_type, published_at, score, snippet`。
其中数值类回答优先引用 `metadata.values` 结构化字段而非 chunk 文本，保证 answer faithfulness 可核对。

---

## 9. 评估数据（eval/ground_truth.jsonl）

20 条 Q&A，每条：

```jsonc
{
  "qid": "q07",
  "question": "2026年10月中旬LME镍的结算价是多少？",
  "expected_doc_ids": ["price-…"],       // recall@5 的金标准
  "expected_answer": "约 15,430 美元/吨…",
  "source_type": "price"                  // 保证三源各≥6条覆盖
}
```

- **recall@5**：top-5 chunk 回链的 `doc_id` 命中 `expected_doc_ids` 即记 1；
- **answer faithfulness**：LLM-as-judge 将回答与召回 chunk 比对打分（0/0.5/1），并抽样人工复核。
