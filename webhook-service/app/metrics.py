from prometheus_client import Counter

# Metrics
events_received = Counter('webhooks_received_total', 'Total webhooks received')
deliveries_successful = Counter('webhooks_deliveries_successful_total', 'Successful deliveries')
deliveries_failed = Counter('webhooks_deliveries_failed_total', 'Failed deliveries')
retry_attempts = Counter('webhooks_retry_attempts_total', 'Total retry attempts')