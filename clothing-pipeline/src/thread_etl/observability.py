"""Structured run logs and bounded CloudWatch metrics without business payloads."""
import json
import logging
import time

LOGGER = logging.getLogger("thread.pipeline")
LOGGER.setLevel(logging.INFO)
if not LOGGER.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(message)s"))
    LOGGER.addHandler(handler)


class RunMonitor:
    def __init__(self, token, pipeline, cloudwatch=None):
        self.token, self.pipeline, self.cloudwatch = token, pipeline, cloudwatch
        self.started = time.monotonic()

    def event(self, stage, state, **fields):
        LOGGER.info(json.dumps(dict(run_id=self.token, pipeline=self.pipeline, stage=stage, state=state, **fields), default=str))

    def metrics(self, **metrics):
        self.event("metrics", "reported", **metrics)
        if self.cloudwatch:
            try:
                self.cloudwatch.put_metric_data(Namespace="Thread/Pipeline", MetricData=[dict(MetricName=name, Value=value,
                    Unit="Seconds" if name.endswith("Seconds") else "Count", Dimensions=[dict(Name="Pipeline", Value=self.pipeline)])
                    for name, value in metrics.items()])
            except Exception as exc:
                LOGGER.error(json.dumps(dict(run_id=self.token, stage="telemetry", state="delivery_failed", error_type=type(exc).__name__)))

    def duration(self):
        return time.monotonic() - self.started
