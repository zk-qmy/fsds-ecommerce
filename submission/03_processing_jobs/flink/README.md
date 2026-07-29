# Flink Job — Streaming Data Problems — 13 pts

**Full write-up:** [`b_schema_pipelines/pipelines/streaming/README.md`](../../../b_schema_pipelines/pipelines/streaming/README.md)
**Fix details + before/after:** [`docs/optimized/spark_flink_pipeline.md`](../../../docs/optimized/spark_flink_pipeline.md)

| Requirement (rubric wording) | Pts | Status |
|---|---|---|
| Baseline→optimized explanation, Flink UI screenshots | 2 | 🟡 procedure documented; **screenshots not yet captured** |
| Handle burst (Problem D) with explanation | 3 | ✅ code + explanation done; **screenshot outstanding** |
| Handle late arrival (Problem E) with explanation | 3 | ✅ code + explanation done; **screenshot outstanding** |
| Handle other streaming problem (chosen: duplicate event_ids, Problem F) | 3 | ✅ code + explanation done; **screenshot outstanding** |
| Window processing — code capture | 2 | ✅ **complete**, no gap — see below |

## Window processing (code capture, not a screenshot — fully satisfied)

```python
# keyBy(customer_id) -> 1-hour tumbling event-time windows -> per-window view count
(
    cleaned_stream
    .key_by(lambda event: event["customer_id"])
    .window(TumblingEventTimeWindows.of(Time.hours(1)))
    .aggregate(ViewCountAggregate(), window_function=ViewCountWindowResult())
)
```

Real windowing branch inside `FlinkStreamPipeline.run()`, event-time semantics (not
processing-time). See the linked write-up for the full pipeline architecture and Flink UI
screenshot checklist.
