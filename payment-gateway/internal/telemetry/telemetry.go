// Package telemetry configures OpenTelemetry tracing and W3C context propagation over Kafka headers.
package telemetry

import (
	"context"
	"os"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// Setup installs the global tracer provider. Without OTEL_EXPORTER_OTLP_ENDPOINT only propagation is set up.
func Setup(ctx context.Context, service string) (func(context.Context) error, error) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" {
		return func(context.Context) error { return nil }, nil
	}
	exp, err := otlptracegrpc.New(ctx)
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(resource.NewSchemaless(attribute.String("service.name", service))),
	)
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}

// HeaderCarrier adapts Kafka record headers to the propagation API.
type HeaderCarrier struct{ R *kgo.Record }

func (c HeaderCarrier) Get(key string) string {
	for _, h := range c.R.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c HeaderCarrier) Set(key, value string) {
	for i, h := range c.R.Headers {
		if h.Key == key {
			c.R.Headers[i].Value = []byte(value)
			return
		}
	}
	c.R.Headers = append(c.R.Headers, kgo.RecordHeader{Key: key, Value: []byte(value)})
}

func (c HeaderCarrier) Keys() []string {
	keys := make([]string, len(c.R.Headers))
	for i, h := range c.R.Headers {
		keys[i] = h.Key
	}
	return keys
}
