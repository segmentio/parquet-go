package benchmark

type Tag struct {
	Name  string `parquet:"name=name, type=UTF8"`
	Value string `parquet:"name=value, type=UTF8"`
}

type Trace struct {
	kafkaTopic     string   //not exporting it to parquet
	KafkaPartition int64    `parquet:"name=kafka_partition, type=INT_64"`
	KafkaOffset    int64    `parquet:"name=kafka_offset, type=INT_64"`
	TimestampMs    int64    `parquet:"name=timestamp_ms, type=INT_64"`
	TraceID        string   `parquet:"name=trace_id, type=UTF8"`
	SpanID         string   `parquet:"name=span_id, type=UTF8"`
	ParentSpanID   string   `parquet:"name=parent_span_id, type=UTF8"`
	NextTraceID    string   `parquet:"name=next_trace_id, type=UTF8"`
	ParentTraceIDs []string `parquet:"name=parent_trace_ids, type=LIST, valuetype=UTF8"`
	Baggage        []Tag    `parquet:"name=baggage, type=LIST"`
	Tags           []Tag    `parquet:"name=tags, type=LIST"`

	// Extracted from the exchange payload. Will be nil if they are not
	// present in the payload or if the payload cannot be unmarshalled.
	MessageID     string `parquet:"name=message_id, type=UTF8"`
	UserID        string `parquet:"name=user_id, type=UTF8"`
	EventType     string `parquet:"name=event_type, type=UTF8"`
	SourceID      string `parquet:"name=source_id, type=UTF8"`
	DestinationID string `parquet:"name=destination_id, type=UTF8"`
	WorkspaceID   string `parquet:"name=workspace_id, type=UTF8"`

	Name         string `parquet:"name=name, type=UTF8"`
	SpanTime     int64  `parquet:"name=span_time, type=INT_64"`     // ms
	SpanDuration int64  `parquet:"name=span_duration, type=INT_64"` // ms

	ExchangeTime     int64 `parquet:"name=exchange_time, type=INT_64"`     // ms
	ExchangeDuration int32 `parquet:"name=exchange_duration, type=INT_32"` // ms

	ExchangeRequestMethod  string            `parquet:"name=exchange_request_method, type=UTF8"`
	ExchangeRequestURL     string            `parquet:"name=exchange_request_url, type=UTF8"`
	ExchangeRequestHeaders map[string]string `parquet:"name=exchange_request_headers, type=MAP, keytype=UTF8, valuetype=UTF8"`
	ExchangeRequestBody    string            `parquet:"name=exchange_request_body, type=UTF8"`

	ExchangeResponseStatusCode uint16            `parquet:"name=exchange_response_status_code, type=UINT_16"`
	ExchangeResponseStatusText string            `parquet:"name=exchange_response_status_text, type=UTF8"`
	ExchangeResponseHeaders    map[string]string `parquet:"name=exchange_response_headers, type=UTF8, keytype=UTF8, valuetype=UTF8"`
	ExchangeResponseBody       string            `parquet:"name=exchange_response_body, type=UTF8"`

	ExchangeErrorType    string `parquet:"name=exchange_error_type, type=UTF8"`
	ExchangeErrorMessage string `parquet:"name=exchange_error_message, type=UTF8"`
}
