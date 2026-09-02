# Node.js Webhook Work Queues: 429 Retry-After Across US/EU SaaS

Short answer: a Node.js webhook queue consumer should treat a `429` and its `Retry-After` value as a durable scheduling decision, release the worker immediately, and let a later worker claim the delivery with the same idempotency key. Keep US and EU delivery pools separate, and make delayed republish a state transition rather than an in-memory timer.

That rule is deliberately boring. It is also what prevents a receiver's throttle from turning into a fleet-wide stall. A worker that sleeps after a `429` still occupies a concurrency slot; a FIFO queue with a throttled item at its head can delay unrelated work; a retry that forgets its delivery identity can create a duplicate side effect after a crash. Those are different failures, but they share one cause: time and ownership were left implicit.

## The incident lesson: a retry is scheduled work, not a paused request

The production scenario to design for is bounded: a SaaS has webhook delivery workers in US and EU pools, and one destination starts returning `429 Too Many Requests`. The failure does not require an exotic outage. One consumer waits in process for the requested delay, other workers continue taking the same destination, and the queue's visible depth falls while the oldest due delivery grows. A restart then loses the waiting timer. If the sender had completed the outbound request before it died but had not persisted completion, the delivery can run again.

Assume duplicates happen.

The invariant is straightforward: a delivery has one stable ID, an immutable payload, an `available_at` time, and a short-lived lease token. A worker may update the row only while its token matches. Success records completion. A rate limit clears the lease and moves `available_at` to the next allowed attempt. An expired lease becomes claimable again. This is at-least-once delivery with a clear recovery path; exactly-once cannot be promised across an external HTTP acknowledgement boundary.

`Retry-After` can carry either a delay in seconds or an HTTP date. RFC 9110 defines both forms. Honor a valid future value from the receiver. When it is absent or unusable, use a documented, capped backoff with jitter; don't silently invent a universal rate-limit policy. The endpoint contract decides whether a `429` is retryable and how a sender should identify a duplicate.

## How should a Node.js webhook queue consumer handle 429 Retry-After delayed republish?

The Node.js process can follow this design even when the claim and send path is shown in Go. The language boundary is not the important one: commit the claim before the HTTP call, preserve the delivery ID on every attempt, and do not hold a database transaction open while a receiver is slow.

PostgreSQL documents `FOR UPDATE SKIP LOCKED` as useful for queue-like access patterns because competing consumers can avoid waiting on rows another consumer has locked. Its result is intentionally inconsistent for general reporting, so use it to claim work and use ordinary queries for operator views. A due-time index and a short transaction make this pattern inspectable during an incident.

```go
func claimDue(ctx context.Context, db *sql.DB, region string, now time.Time) (*Delivery, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	leaseToken := uuid.NewString()
	row := tx.QueryRowContext(ctx, `
		SELECT id, target_url, payload, attempt
		FROM webhook_deliveries
		WHERE region = $1
		  AND state = 'pending'
		  AND available_at <= $2
		  AND (lease_until IS NULL OR lease_until < $2)
		ORDER BY available_at, id
		FOR UPDATE SKIP LOCKED
		LIMIT 1`, region, now)

	var d Delivery
	if err := row.Scan(&d.ID, &d.TargetURL, &d.Payload, &d.Attempt); err != nil {
		return nil, err
	}
	d.LeaseToken = leaseToken
	_, err = tx.ExecContext(ctx, `
		UPDATE webhook_deliveries
		SET lease_token = $2, lease_until = $3
		WHERE id = $1`, d.ID, leaseToken, now.Add(30*time.Second))
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &d, nil
}
```

The sender adds the stable delivery ID as the receiver's agreed idempotency key. After a `429`, it parses the header, then executes one guarded update: increment the attempt count, set `available_at`, and clear the lease only if the token still matches. A stale worker must not be able to overwrite a newer lease.

```go
func retryAt(value string, now time.Time) (time.Time, bool) {
	if seconds, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64); err == nil && seconds >= 0 {
		return now.Add(time.Duration(seconds) * time.Second), true
	}
	when, err := http.ParseTime(value)
	if err != nil || !when.After(now) {
		return time.Time{}, false
	}
	return when, true
}

func reschedule429(ctx context.Context, db *sql.DB, d Delivery, due time.Time) error {
	result, err := db.ExecContext(ctx, `
		UPDATE webhook_deliveries
		SET available_at = $3, attempt = attempt + 1,
		    lease_token = NULL, lease_until = NULL
		WHERE id = $1 AND lease_token = $2`, d.ID, d.LeaseToken, due)
	if err != nil {
		return err
	}
	updated, err := result.RowsAffected()
	if err != nil || updated != 1 {
		return ErrLeaseLost
	}
	return nil
}
```

This is delayed republish without a second copy of the payload. Calling it a republish is fine operationally; the durable action is updating the existing delivery's next eligible time. A separate audit record can capture each attempt without making the scheduling state ambiguous.

## Regional fairness needs admission controls, not larger worker pools

Use the residency region as a routing boundary before a delivery reaches a worker. US backlog must not consume EU concurrency, and a hot EU tenant must not consume every EU slot. Within each region, rotate among tenants with due work and cap both tenant concurrency and destination-host concurrency. The second cap matters because several tenants can send to the same receiver.

There is no universally correct limit. Derive it from the receiver contract and load testing, then make the setting observable and adjustable through normal deployment controls. A queue-wide rate limit is simpler, but it is a poor fit when one destination is throttled and another destination is healthy. The catch is that strict fairness can reduce peak throughput when traffic is genuinely uniform; use it where isolation and predictable latency matter more than squeezing every slot.

Don't use cron as the per-delivery delay mechanism. Cron is useful for a periodic reconciliation sweep: find overdue rows, expired leases, and terminal records that need review. Its cadence is coarse and batchy, which is a poor match for a receiver-supplied retry time. A database table is also not suitable when timer volume or sub-second precision dominates the workload; choose a durable delayed-message system in that case, while retaining the same ID, lease, and idempotency rules.

## What should the runbook and tests prove?

During a page, start with the oldest *due* delivery by region. Scheduled work in the future is not overdue. Then check whether leases are expiring, whether a particular tenant or destination owns the backlog, and whether successful completions are advancing. This sequence separates a deliberate `Retry-After` delay from a stuck claim path without guessing from raw queue depth.

The useful signals are due-age percentile by region, count of expired leases, attempts per completed delivery, terminal delivery count, and `429` rate by destination and tenant. Page on overdue age against an objective, not on every rate-limit response. A receiver may correctly ask for a ten-minute delay; alerting on the response alone turns compliant behavior into noise.

The investigation should also preserve the order of evidence. Take one overdue delivery ID and follow it from enqueue through its current `available_at`, lease token, attempt history, destination host, and final state. Compare that record with a delivery to the same host from a different tenant and with a delivery to a different host from the same tenant. Those two comparisons tell an operator which boundary is applying pressure: scheduling, tenant admission, or destination admission. Next, confirm that the worker which held the lease either wrote a guarded terminal update or allowed the lease to expire; a missing terminal state is not permission to manually replay a whole tenant. Inspect the receiver's stated delay before changing concurrency. Increasing workers while honoring no per-destination limit can turn a small 429 cluster into a synchronized retry wave, and decreasing every regional worker can make healthy destinations miss their own delivery objective. The record-level trail is slower than staring at a dashboard, but it produces an action that is reversible and scoped. Only after that evidence is clear should the runbook change a limit, pause a tenant, or replay a bounded set of deliveries.

Test the state machine with a frozen clock. Exercise seconds and HTTP-date forms of `Retry-After`, a missing header, a worker exit after external acceptance, lease expiry, and two concurrent claimers. For each case, assert that a valid token changes state once, an expired lease can be recovered, and a stale token cannot mark a later worker's delivery complete. Add a deployment test as well: readers should tolerate old and new state values while the schema change is rolling out.

The design will not force a receiver to deduplicate. If the receiver does not honor an agreed idempotency key, the sender can contain duplicates but cannot eliminate their external side effects. That boundary belongs in the integration contract and the operational runbook, not in an optimistic queue metric.

## References

- https://www.rfc-editor.org/rfc/rfc9110#field.retry-after
- https://www.postgresql.org/docs/current/sql-select.html
- https://en.wikipedia.org/wiki/Cron
