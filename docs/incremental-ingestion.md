# 多数据源增量采集能力说明

源端与目标端均以**本地方式模拟**，不依赖外部数据库 / 消息中间件，即可验证
重复采集、断点续传、水位回退拒绝、并发采集、部分失败隔离与重捞。

## 1. 模块结构

```
model/       原始记录、归一记录、拒绝原因枚举、批次/重捞结果、失败策略
normalize/   严格归一器：逐字段校验，给每种问题不同的拒绝原因
source/      SourceConnector 接口与注册表；local/ 下为两种本地来源
watermark/   水位存储：内存版（InMemory）与文件版（File），均单调递增 + CAS
sink/        LocalSink：按 dedupKey 幂等写入，可注入临时/永久故障
skipped/     失败隔离区：内存版与文件版（JSON），支持重启后重捞
retry/       单条写入的重试策略与执行器
engine/      IngestionEngine：编排整个采集流程
demo/        LocalIngestionDemo：纯本地可运行的端到端演示
```

两种本地来源：

| 类型 | 类 | 模拟对象 | 特点 |
| --- | --- | --- | --- |
| `local-table` | `LocalTableSource` | 数据库表 | 行是强类型 Map，sequence 为 long |
| `local-file` | `LocalFileSource` | JSON 行文件 | 字段为宽松类型，可构造缺失/类型错/格式错 |

新来源只需实现 `SourceConnector` 并注册到 `SourceRegistry`，
归一与后续流程完全复用，无需改动引擎。

## 2. 采集流程

`IngestionEngine.ingestOnce(sourceType, sourceId, batchSize, strategy)`：

1. **恢复水位**：读取该来源已提交水位 W（不存在则为 0），
   并校验启动点不能低于 W，否则抛 `WatermarkRegressionException` 明确拒绝。
2. **增量拉取**：连接器返回 `sequence > W` 的记录，按序号升序、最多 batchSize 条。
3. **逐条归一**（`StrictRecordNormalizer`）：
   - 通过 -> `NormalizedRecord`；
   - 不通过 -> 记录到 `RejectedRecord`，带**互不相同**的 `RejectReason`，
     不做静默跳过。
4. **幂等写入目标端**：目标端按 `dedupKey = sourceId:recordId` 去重，
   - 首次写入记为「成功」；
   - 已存在记为「重复」（重复采集的兜底，不算失败、不产生重复数据）。
5. **失败处理**：写入按 `RetryPolicy(maxAttempts, backoffMillis)` 单条重试；
   重试耗尽后由 `FailureStrategy` 决定：
   - `SKIP`：写入隔离区（`SkippedStore`），本批继续，水位可越过；
   - `FAIL`：记为失败，**水位在该记录处断开**，但同批其它记录仍会被处理
     （已写入的下一轮靠幂等去重，不漏不重）。
6. **水位推进**：只推进到「从 W+1 开始的连续已确认前缀」的末端。
   - 成功、幂等重复、SKIP 隔离、序号可解析的归一拒绝都算「已确认」；
   - FAIL 失败不在集合中，因此形成缺口，水位停在缺口前一位；
   - 序号无法解析的拒绝记录不占序号位置，每轮会被再次拉到并明确拒绝。
7. **条件提交**：`compareAndAdvance(sourceId, expected=W, next)`，
   仅当当前水位仍等于本轮开始读到的 W 才提交；next 小于当前水位则判为**回退**并抛异常。

### 拒绝原因（`RejectReason`）

| 枚举值 | 含义 |
| --- | --- |
| `SEQUENCE_MISSING` | 缺少采集序号 sequence |
| `SEQUENCE_NOT_A_NUMBER` | sequence 不是可解析数字 |
| `MISSING_ID` | 缺少记录主键 id |
| `ID_TYPE_MISMATCH` | id 类型不是字符串 |
| `MISSING_NAME` | 缺少 name 字段 |
| `NAME_BLANK` | name 为空白字符串 |
| `EVENT_TIME_MALFORMED` | eventTime 无法解析为 ISO-8601 时间 |

归一拒绝是**永久判定**：重采同样记录会得到同样原因，所以有序号的拒绝允许水位越过；
它不是「跳过」，统计上单列 `rejectedCount`，日志同时打印输入数据与判定依据。

## 3. 断点与重试规则

- **水位语义**：`watermark = 已确认处理完的最大连续序号`，单调递增，只进不退。
- **重启恢复**：`FileWatermarkStore` 每个来源一个文件，先写 `.tmp` 再原子 rename；
  重启后只认磁盘水位。`FileSkippedStore` 以 JSON 持久化隔离记录。
- **水位回退**：启动点低于已提交水位、或 CAS 提交一个更小的值，
  都抛 `WatermarkRegressionException`，消息含来源、已提交水位、请求水位。
- **同来源并发**：引擎对每个 sourceId 维护一把锁，同一来源的并发拉取**串行化**，
  不同来源仍可并行；叠加水位 CAS，断点不会相互覆盖，最终水位收敛、目标端恰好一份。
- **重复采集**：即使水位丢失后从零全量重拉，目标端 `putIfAbsent` 幂等，
  旧数据全部命中「重复」，不重不漏。
- **单条重试**：只对写入动作重试；`RetryPolicy.noRetry()` 表示不重试直接进入策略分支。
- **失败隔离与重捞**：
  - `SKIP` 后的记录进入隔离区，同 dedupKey 反复失败只保留一条（更新尝试次数）；
  - `reclaimSkipped(sourceId)` 重捞：成功则移出隔离区，仍失败则保留、下次再捞；
  - 重捞写入同样幂等，不会重复写入。

## 4. 本地验证

前置：JDK 21、Maven。

### 4.1 运行全部单元测试

```bash
mvn test
```

测试与重点场景对应：

| 测试类 | 覆盖点 |
| --- | --- |
| `NormalizationRejectionTest` | 7 种拒绝原因逐一区分 + 合法记录通过 |
| `WatermarkPersistenceTest` | 条件提交、回退拒绝、启动点校验、文件版重启恢复 |
| `IncrementalIngestionTest` | 分批续传不重不漏、重复整批采集幂等、SKIP+重捞、重试成功、FAIL 阻塞水位 |
| `ConcurrentIngestionTest` | 8 线程并发拉同一来源（200 条），结果一致、无重复无漏、水位收敛 |
| `EndToEndRestartRecoveryTest` | 两类型来源 + 混合拒绝原因 + 文件持久化重启恢复 + 回退拒绝 + 重捞 |

单测日志会对每条输入打印「输入数据 + 判定依据/拒绝原因」，
例如：`记录被拒绝 | 输入={...} | 判定原因=MISSING_NAME | 细节=...`。

### 4.2 运行本地端到端演示

IDE 中直接运行：

```
com.github.h3yow.incrementalsourceingestionjava.demo.LocalIngestionDemo#main
```

或命令行：

```bash
mvn -q compile
mvn -q dependency:build-classpath -Dmdep.outputFile=/tmp/cp.txt
java -cp "target/classes:$(cat /tmp/cp.txt)" \
  com.github.h3yow.incrementalsourceingestionjava.demo.LocalIngestionDemo
```

演示会在临时目录生成 `watermark-demo-file.txt` 与 `skipped-demo-file.json`，
依次展示：首次采集（成功/拒绝/跳过统计）-> 重捞隔离记录 -> 重启后从磁盘水位恢复。

### 4.3 手动接入一个新来源的最小步骤

1. 实现 `SourceConnector`（`sourceType / sourceId / fetch(afterSequence, limit)`）；
2. `registry.register(connector)`；
3. 调 `engine.ingestOnce(type, id, batchSize, FailureStrategy.SKIP)`；
4. 需要补偿时调 `engine.reclaimSkipped(id)`。
