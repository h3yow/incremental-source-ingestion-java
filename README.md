# 增量采集能力（源端 / 目标端本地模拟）

在 Spring Boot 骨架上新增的一套**可接多种数据源的增量采集**能力。源端、目标端、水位、隔离区
都先用本地（内存 / 文件）方式模拟，但接口设计面向真实数据源；重点保证：

- 多种来源类型可注册、可按各自字段约定**统一归一**，坏数据**不静默跳过**，拒绝原因互不相同；
- 采集水位可记录、可恢复，**重启断点续传、重复采集不重不漏**；
- 水位回退被明确拒绝；同一来源并发拉取由 CAS 保证**断点不互相覆盖、结果收敛一致**；
- 单条失败被**隔离**，不拖垮整批；可按策略重试 / 跳过；批末给出成功、失败、跳过、重复计数；
- 被跳过的数据之后可**重新捞回**，且不重复写入。

---

## 1. 整体流程

```
                 ┌──────────────────────────────────────────────────────────┐
                 │                     IngestionService                      │
 源端(本地模拟)   │                                                            │   目标端(本地文件)
 InMemorySource  │  1. load 水位  ──► 2. fetch(afterCursor) 增量拉取(游标升序) │   FileTargetSink
 Connector  ───► │  3. Normalizer 按来源类型约定统一归一(校验,坏数据即拒绝)     │ ──► 按 businessKey
                 │  4. RetryPolicy 决策：重试 / 失败隔离 / 跳过隔离            │     幂等写入
                 │  5. TargetSink 幂等 put（已存在 -> DUPLICATE 去重）         │
                 │  6. 仅对成功/去重记录推进水位，逐条计算，批末 CAS 提交一次    │
                 │  7. 失败记录进 IsolationStore，等待 recoverPending 捞回     │
                 └──────────────────────────────────────────────────────────┘
                         │ 文件落盘(data/)                │ 文件落盘(data/)
                  FileWatermarkStore              FileIsolationStore
```

包结构：

| 包 | 职责 |
|----|------|
| `model` | `RawRecord` / `NormalizedRecord` / `RejectionReason` / 异常 |
| `source` | `SourceType`（字段约定）、`SourceConnector`（源端）、`SourceRegistry`（注册中心） |
| `normalize` | `Normalizer`：多来源统一归一 + 确定性校验 |
| `watermark` | `Watermark` / `WatermarkStore`（CAS 提交）/ 回退、冲突异常 |
| `sink` | `TargetSink`：按业务主键幂等写入 |
| `isolation` | `IsolationRecord` / `IsolationStore`：坏数据隔离与捞回 |
| `policy` | `RetryPolicy` / `DefaultRetryPolicy`：单条失败重试与隔离策略 |
| `service` | `IngestionService` 编排、`BatchResult`/`ItemResult` 计数明细 |
| `local` | 上述接口的本地（内存/文件）实现与 Spring 装配、`demo` 演示入口 |

### 多来源注册与统一归一

- `SourceType` 描述一种来源类型的字段约定：业务主键字段名、事件时间字段名、必填载荷字段。
- `SourceRegistry` 可注册多种 `SourceType` 和多个 `SourceConnector`（同一类型可有多个实例）。
- `Normalizer.normalize(raw)` 先按 `raw.sourceType()` 找约定，再统一归一为 `NormalizedRecord`。

---

## 2. 拒绝原因（互不相同，绝不静默跳过）

归一化遇到字段缺失 / 类型对不上 / 格式不完整时，抛 `RejectedRecordException`，
携带互斥的 `RejectionReason`，日志打印**输入数据 + 判定依据**：

| RejectionReason | 触发条件 |
|-----------------|----------|
| `FIELDS_MISSING` | `fields` 整体为 null |
| `MISSING_BUSINESS_KEY` | 主键字段不存在 |
| `EMPTY_BUSINESS_KEY` | 主键为空白字符串 |
| `BUSINESS_KEY_TYPE_MISMATCH` | 主键是对象/数组等无法归一的类型 |
| `MISSING_CURSOR` | 未提供采集游标 |
| `CURSOR_TYPE_MISMATCH` | 游标无法解析为长整型 |
| `EVENT_TIME_MALFORMED` | 事件时间存在但无法解析（格式不完整） |
| `MISSING_REQUIRED_PAYLOAD_FIELD` | 约定的必填载荷字段缺失或为空白 |

> 事件时间允许缺失（归一为 `null`），但**只要给了就必须可解析**，否则拒绝。

---

## 3. 断点（水位）与并发规则

`Watermark(sourceName, position, version, lastUpdatedAt)`：

- **提交采用 CAS**：`commit(sourceName, expectedVersion, newPosition)`。
  - 磁盘实际版本 ≠ `expectedVersion` → 抛 `WatermarkConflictException`，
    说明有并发拉取抢先提交，本次断点**不被覆盖**；调用方应基于新水位重试。
  - `newPosition < 当前 position` → 抛 `WatermarkRegressionException`，
    **拒绝从来路不明的中间点接着跑**，且不改写水位。
  - 校验通过后 `version + 1`，写临时文件再原子移动落盘。
- **逐条推进、批末一次提交**：只有 `SUCCESS` / `DUPLICATE` 的记录才参与推进水位；
  被隔离 / 跳过的记录游标不越过，保证坏数据修好后不会因水位前移而丢失。
- **重启恢复**：水位、目标端数据、隔离记录全部落盘到 `data/`。
  用全新 store 实例（模拟新 JVM）启动即从断点继续。
- **并发收敛**：多个线程对同一来源拉取时，可能交错处理同一批记录；
  目标端按业务主键幂等（重复处理命中去重），水位 CAS 串行化提交，
  最终目标端**恰好 N 条、水位到最大游标**，不重不漏（见 `ConcurrentIngestionTest`）。

---

## 4. 单条失败、重试与捞回

单条记录结果 `ItemOutcome`：`SUCCESS` / `FAILED` / `SKIPPED` / `DUPLICATE`。

- **确定性失败**（归一化拒绝）：重试无意义，立即隔离为 `FAILED`，计入 failed。
- **瞬时失败**（目标端超时等，由 `RecordFailureInjector` 模拟）：
  按 `DefaultRetryPolicy` 指数退避重试，默认最多 3 次；
  重试耗尽仍失败 → 隔离为 `SKIPPED`，计入 skipped，**不写入目标端**。
- **一条失败不影响同批其他记录**；`BatchResult` 给出 total/success/failed/skipped/duplicate。
- **重新捞回**：`recoverPending(sourceName)` 取出隔离区 `FAILED/SKIPPED` 记录原样重处理：
  - 成功或确认重复 → 标记隔离记录为 `RECOVERED`，目标端幂等（不重复写）；
  - 仍失败 → 更新尝试次数与状态，继续留在隔离区，下次还能再捞。

---

## 5. 本地验证

### 跑单元测试（推荐入口）

```bash
mvn test
```

重点测试（日志含每条输入与判定依据）：

| 测试类 | 覆盖场景 |
|--------|----------|
| `NormalizerTest` | 8 类拒绝原因互不相同；合法记录归一成功 |
| `WatermarkTest` | 水位回退拒绝、CAS 版本冲突拒绝、重启恢复 |
| `ConcurrentIngestionTest` | 同一来源 8 线程并发，结果收敛、不重不漏、冲突可见 |
| `IngestionReplayTest` | 重复采集幂等；单条瞬时失败重试成功；持续失败跳过隔离；捞回成功且不重复 |
| `RestartRecoveryTest` | 分批采集 + 全新实例重启后断点续传 |

### 端到端演示（落盘可见）

```bash
mvn spring-boot:run -Dspring-boot.run.arguments="--spring.profiles.active=demo --server.port=0"
```

运行后查看 `data/` 目录：

- `watermark-demo-orders.json`：水位（位置、版本号）；
- `sink-demo-orders.json`：目标端按业务主键去重后的数据；
- `isolation/*.json`：被隔离记录（含原始快照、拒绝原因、尝试次数、状态）。

落盘目录可用 `--ingestion.data-dir=/path/to/dir` 调整（默认 `data/`，已在 `.gitignore` 忽略）。

> 演示进程是常驻 Web 应用，看完文件后手动停止即可（`Ctrl+C`）。
