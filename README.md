# incremental-source-ingestion-java

支持多种本地模拟数据源的增量采集骨架：统一归一、水位断点续传、
重复采集幂等、单条失败隔离与重捞、同来源并发安全。

详细说明见：[docs/incremental-ingestion.md](docs/incremental-ingestion.md)

## 快速开始

```bash
mvn test          # 19 个用例：拒绝原因 / 水位回退 / 并发采集 / 部分失败 / 重启恢复
```

本地端到端演示（不依赖外部组件）：运行
`com.github.h3yow.incrementalsourceingestionjava.demo.LocalIngestionDemo#main`。

## 核心保证

- **不静默丢数据**：字段缺失/类型错/格式不完整返回 7 种明确拒绝原因。
- **不重不漏**：目标端按 `sourceId:recordId` 幂等；重复采集只产生「重复」计数。
- **水位只进不退**：回退或从低于已提交水位的位置启动一律抛
  `WatermarkRegressionException`；文件持久化支持进程重启恢复。
- **并发一致**：同一来源串行化 + 水位 CAS，断点不相互覆盖。
- **失败隔离**：单条失败不拖垮整批，可重试、可跳过，跳过后可重捞且不重复写入。
