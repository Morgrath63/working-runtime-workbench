# Idempotent Node.js SaaS Processing for Delayed Delivery

Short answer: persist each webhook delivery and its stable idempotency key before attempting it; use a periodic scheduler for coarse, low-volume retry work and a queue only when its dispatch and timing guarantees justify the extra operational surface.

The scheduler is rarely the part that decides whether a retry is safe. A process can stop after a remote endpoint accepts a request but before the sender records success. On the next run, the sender has no proof of the outcome, so it must try again. That is normal at-least-once delivery, not an exceptional edge case. The design has to make a second attempt harmless.

This is the incident lesson worth keeping in a runbook: acceptance, attempt, and completion are different states. Treating them as one state creates the quiet failure where an event is accepted locally yet never leaves, or is sent twice after a restart. The fix starts with durable state and a receiver contract, then picks a wake-up mechanism.

## What should a Node.js SaaS use for delayed webhook delivery retries and idempotent processing?

Start with a delivery table or other durable store owned by the application. A transaction that records the business event should also record a pending delivery, its destination, its next eligible attempt, and an idempotency key that never changes for that delivery. A worker claims due rows for a bounded lease, sends them, and records a final state only after observing a response.

For a small SaaS with delays measured in minutes, a periodic scan is often the simplest option. It is easy to inspect: the retry schedule is a query, and the oldest pending row is a useful alert. Index the fields used to find work, particularly the state and next-attempt time. Do not let a fixed batch ordered by insertion ID repeatedly spend its entire budget on one failing destination; order due work by its next attempt time and apply backoff per delivery.

A message queue becomes attractive when bursts require many consumers, when the requested delay is shorter than the scan interval, or when backpressure must be separated from the application database. It does not remove the durable-record requirement. A consumer can receive the same message again after a lease or acknowledgement boundary, and a producer can fail between recording intent and publishing. Keep the delivery record as the authority; the queue is a dispatch signal.

| Need | Favor | Reason to reconsider |
|---|---|---|
| Minute-level delay and modest volume | Periodic database scan | The promised delivery time is tighter than the interval |
| Independent consumer scaling or burst absorption | Queue plus delivery record | The queue is being treated as the only audit history |
| Safe replay after an uncertain request | Stable key and durable receiver record | The receiver cannot make repeated keys harmless |

There is no universal cheapest choice. Cost includes the service, but also on-call time for retention, visibility windows, dead-letter policy, alarms, upgrades, and replay procedures. Measure the rate, delay precision, retention period, and recovery objective first. A boring scan that meets them is often the smaller system.

## The invariant that prevents a quiet loss

Write down the state machine before writing the worker. `pending` means a delivery is owed. `claimed` means one worker may attempt it until a lease expires. `delivered` means a successful response was observed and recorded. `failed` is a deliberately terminal outcome with a reason suitable for review. An expired claim returns to eligible work rather than disappearing.

The important ordering is simple: commit intent before dispatch; mark completion after the response. Never mark a row delivered merely because an HTTP request was constructed or placed on a socket. A timeout is ambiguous; the remote side may have completed the request while the response was lost. Imagine a worker posts a renewal event, the receiving service commits its side effect, and the connection closes before the response reaches the worker. Retrying is the only honest sender action, because marking the delivery final would silently lose a request if the receiver never saw it. The repeated post must therefore carry the same key. The receiving service atomically records that key before its business side effect, then returns success for a repeated key. A delivery ID handles transport retries; a business event ID can additionally protect against the same event being emitted through separate delivery records. Which identifier belongs in the receiver's uniqueness constraint depends on the contract, so document the distinction and test both paths.

Do not guess.

Webhook authentication belongs beside this logic. RFC 2104 specifies HMAC as a keyed message-authentication construction. Sign a defined byte sequence, include enough metadata for the receiver to reject stale requests under its own policy, and compare the expected and supplied authentication values without leaking a partial-match result. Retrying an unsigned or ambiguously signed body turns a delivery concern into a security concern.

Small rule. **A successful enqueue is never proof of successful delivery.**

## A focused worker path in Go

The transport loop should stay independent of the scheduler. The following sketch assumes `claimDue`, `markDelivered`, and `reschedule` perform transactional state changes in the durable store. The lease duration, HTTP timeout, retry cap, and backoff policy are application decisions; they should be visible in configuration and in the runbook.

```go
package delivery

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

type Delivery struct {
	ID             string
	Endpoint       string
	Payload        []byte
	IdempotencyKey string
}

type Store interface {
	MarkDelivered(context.Context, string, int) error
	Reschedule(context.Context, string, string) error
}

func Deliver(ctx context.Context, store Store, client *http.Client, key []byte, d Delivery, now time.Time) error {
	timestamp := strconv.FormatInt(now.Unix(), 10)
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(timestamp + "."))
	_, _ = mac.Write(d.Payload)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.Endpoint, bytes.NewReader(d.Payload))
	if err != nil {
		return store.Reschedule(ctx, d.ID, "request construction failed")
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", d.IdempotencyKey)
	req.Header.Set("X-Delivery-Timestamp", timestamp)
	req.Header.Set("X-Delivery-Signature", hex.EncodeToString(mac.Sum(nil)))

	resp, err := client.Do(req)
	if err != nil {
		return store.Reschedule(ctx, d.ID, "delivery outcome unknown")
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))

	if resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices {
		return store.MarkDelivered(ctx, d.ID, resp.StatusCode)
	}
	return store.Reschedule(ctx, d.ID, fmt.Sprintf("received status %d", resp.StatusCode))
}
```

The call site needs a nonzero HTTP client timeout or a context deadline. It also needs a bounded retry policy: retrying every response forever is a way to turn a retired endpoint into permanent load. Keep the response status, attempt count, age, and next attempt with the row. Those fields make a replay and a support investigation tractable without reconstructing history from logs.

## When is a queue the better wake-up mechanism?

Choose a scheduler scan when one database can safely hold the retry state, latency can tolerate its interval, and operators need direct visibility into work that is due, leased, or terminal. It is particularly useful while the system is still changing because the data model is the operational interface.

Choose a queue for independent consumer scaling, burst absorption, and fine-grained dispatch. Still make handlers idempotent and still retain an audit record. Priority settings need care: RabbitMQ documents that priority queues add resource cost, and messages already delivered to a consumer are not reordered by later arrivals. Priority is not a substitute for an explicit fairness policy or per-destination concurrency limits.

The catch is that a scheduler scan is not suitable when the product promises sub-interval timing or demand exceeds the database's safe claim rate. A queue is not suitable as the sole record when compliance, support, or replay requires an authoritative delivery history. In both cases, avoid making an unbounded queue depth the primary alert. Alert on the age of the oldest eligible delivery, lease-expiration counts, terminal failures by destination, and the gap between accepted and completed deliveries.

Test the awkward boundaries before deployment: process termination after send, duplicate dispatch, expired leases, a receiver that records the key then crashes before responding, and a long outage at one destination. The expected result is not exactly-once magic. It is a delivery system whose uncertainty is contained, observable, and safe to replay.

## References

- https://www.rfc-editor.org/rfc/rfc2104
- https://www.rabbitmq.com/docs/priority
