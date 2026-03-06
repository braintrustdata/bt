import { getNodeAutoInstrumentations } from "@opentelemetry/auto-instrumentations-node";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-proto";
import { NodeSDK } from "@opentelemetry/sdk-node";
import { SimpleSpanProcessor } from "@opentelemetry/sdk-trace-node";

export const sdk = new NodeSDK({
  serviceName: "bt-eval-vite",
  traceExporter: new OTLPTraceExporter(),
  instrumentations: getNodeAutoInstrumentations(),
  spanProcessors: [new SimpleSpanProcessor(new OTLPTraceExporter())],
});

sdk.start();
