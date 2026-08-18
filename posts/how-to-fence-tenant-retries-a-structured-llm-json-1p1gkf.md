# How to Fence Tenant Retries — A Structured LLM JSON Worker Pipeline

Short answer: assign one durable business key to each tenant, source revision, and extraction schema; claim that key before calling the LLM, then commit the validated JSON record, attempt ledger, and terminal job state in one transaction.

Do not use a webhook delivery ID as that key. Deliveries identify transport attempts, while the record represents a business result. Mixing the two is how a perfectly ordinary retry becomes a second row and a second unassigned inference charge.

I've been paged for both missed scheduled jobs and duplicate deliveries. The useful incident pattern was a worker that accepted a webhook, completed an expensive extraction, wrote the answer, and lost its acknowledgement during shutdown. The sender retried. A second worker saw a new delivery ID and repeated the whole path. Nothing exotic had failed: each component had followed its local contract, but the system had no shared definition of “this extraction already exists.” In an edtech knowledge-base service, that can leave two answers for the same handbook revision and make the district's usage ledger disagree with its visible records.

That incident left one invariant: retries may create attempts, but only the business identity may create a result.

## How should an LLM structured extraction webhook worker handle retries?

Start by naming the unit of work. For a private knowledge-base question-answering pipeline, a practical identity tuple is `tenant_id + source_document_id + source_revision + extraction_schema_version`. The tenant belongs in the key because identical document identifiers from two districts must never collide. The source revision ensures an amended policy can produce a fresh result. The schema version makes an intentional output-contract change a new job rather than a silent overwrite.

Hashing the tuple is an implementation detail; preserving its meaning is the design decision. Store the readable fields beside the hash so an operator can explain a collision report at 03:00 without reverse-engineering application logs.

The job then needs a small state machine: `pending`, `running`, `complete`, and `retryable`. A worker atomically claims `pending` or an expired `running` lease and receives an attempt number. Completion is accepted only from the current attempt. That last check is a fencing token: if worker A pauses past its lease and worker B takes over, A cannot wake up later and overwrite B's result. A `409 Conflict` is a reasonable internal response to that stale completion; the webhook edge can still return `202 Accepted` once durable ownership of the business key exists.

Validate before the terminal commit. JSON syntax alone is weak evidence. Check required fields, types, allowed values, source identifiers, and any confidence representation your contract defines. Treat prose around the object as invalid output rather than trimming until it parses. Prompt-injection defenses also belong before inference and before display, especially when the private source text is untrusted content rather than an instruction channel.

Keep scheduling separate from identity. Per-tenant queues, weighted fair scheduling, or concurrency caps can decide when a job runs; none of them should decide whether the result is new. This separation matters for cost visibility. Record every inference attempt against the tenant and business key, including attempts that do not publish a result, then make the published record point to the winning attempt. The dashboard can now answer two different questions: “How many current answers exist?” and “How much work did this tenant cause?”

One key. Many attempts. One result.

## Put the idempotency fence before inference

The following Go program is intentionally small enough to run, but its transitions mirror the durable design. The mutex stands in for a database transaction. In production, the `jobs`, `records`, and `usage` mutations must share one durable transactional boundary, and the business key needs a unique constraint. A process-local map cannot protect two replicas or survive a restart.

```go
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

var errStaleClaim = errors.New("stale or completed claim")

type Work struct {
	TenantID     string
	DocumentID   string
	Revision     string
	SchemaVersion string
}

type Result struct {
	Answer   string   `json:"answer"`
	Citations []string `json:"citations"`
}

type Job struct {
	State      string
	Attempt    int
	LeaseUntil time.Time
}

type Usage struct {
	TenantID string
	JobKey   string
	Attempt  int
	Units    int
}

type Store struct {
	mu      sync.Mutex
	jobs    map[string]Job
	records map[string]Result
	usage   map[string]Usage
}

func businessKey(w Work) string {
	// Length-prefixing would also work; NUL separators keep this example unambiguous.
	raw := w.TenantID + "\x00" + w.DocumentID + "\x00" + w.Revision + "\x00" + w.SchemaVersion
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

func (s *Store) claim(key string, now time.Time, lease time.Duration) (int, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	job, exists := s.jobs[key]
	if exists && job.State == "complete" {
		return job.Attempt, false
	}
	if exists && job.State == "running" && now.Before(job.LeaseUntil) {
		return job.Attempt, false
	}

	job.Attempt++
	job.State = "running"
	job.LeaseUntil = now.Add(lease)
	s.jobs[key] = job
	return job.Attempt, true
}

func (s *Store) complete(w Work, key string, attempt, units int, rawJSON []byte) error {
	var result Result
	if err := json.Unmarshal(rawJSON, &result); err != nil {
		return fmt.Errorf("invalid extraction JSON: %w", err)
	}
	if result.Answer == "" || len(result.Citations) == 0 {
		return errors.New("extraction violates the output contract")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	job := s.jobs[key]
	if job.State != "running" || job.Attempt != attempt {
		return errStaleClaim
	}
	usageKey := fmt.Sprintf("%s:%d", key, attempt)
	s.records[key] = result
	s.usage[usageKey] = Usage{TenantID: w.TenantID, JobKey: key, Attempt: attempt, Units: units}
	job.State = "complete"
	s.jobs[key] = job
	return nil
}

func main() {
	store := &Store{
		jobs:    make(map[string]Job),
		records: make(map[string]Result),
		usage:   make(map[string]Usage),
	}
	work := Work{
		TenantID: "district-17", DocumentID: "student-handbook",
		Revision: "rev-42", SchemaVersion: "answer-v3",
	}
	key := businessKey(work)
	attempt, acquired := store.claim(key, time.Now(), 30*time.Second)
	if !acquired {
		return
	}

	modelOutput := []byte(`{"answer":"Use the current attendance policy.","citations":["handbook#attendance"]}`)
	if err := store.complete(work, key, attempt, 812, modelOutput); err != nil {
		panic(err)
	}
	_, duplicateAcquired := store.claim(key, time.Now(), 30*time.Second)
	fmt.Printf("records=%d usage_entries=%d replay_acquired=%t\n",
		len(store.records), len(store.usage), duplicateAcquired)
}
```

The example prints one record, one usage entry, and `replay_acquired=false`. The `812` units are fixture data for exercising tenant attribution, not a benchmark or pricing claim. A real adapter should take usage from the runtime's response instead of estimating from string length. I'm not sure every runtime exposes a stable request identifier for later reconciliation; verify that contract before deciding whether a provider receipt can close the narrow crash window between remote completion and your local transaction.

There is another subtlety. If a lease expires while an inference call is still running, a replacement attempt can incur additional usage even though fencing prevents a duplicate record. Set the lease from observed tail latency, renew it only while the worker still owns the attempt, and alert on takeovers. Idempotent storage protects correctness; it does not make remote computation exactly once.

## Compare the failure boundary, not the product name

Different primitives stop different duplicates. A durable database unique constraint can enforce one result per business key across replicas. A queue's deduplication feature can suppress some repeated messages before delivery. A distributed lock can reduce concurrent work. Only the first directly protects the record invariant, so queue or lock deduplication is an optimization around the transactional fence, not a replacement for it.

Three common implementations illustrate the boundary. PostgreSQL can combine a unique constraint, row locking, and the result write in a transaction. Redis `SET` with `NX` and expiration is useful for a time-bounded claim, but a lock expiring does not prove the old worker stopped; retain the database fencing token. NATS JetStream can deduplicate messages within its configured duplicate window, while a webhook replay outside that window still needs the business key. These are factual capability boundaries, not a ranking. The right component depends on the store already trusted for tenant records, the queue's delivery contract, and how much operational state the team is prepared to own.

The runbook should expose the invariant directly. Track claims, lease takeovers, stale completions, schema-validation failures, attempts per completed key, and usage units by tenant. Log the business key, tenant, attempt, and source revision on every transition, but do not log private source text or extracted answers. Page on sustained stale completions or a widening gap between inference attempts and completed records; a single retry is diagnostic data, not automatically an incident.

Deploy schema changes with both readers active. Create a new schema version, let new jobs write it, verify consumers, and only then retire the old reader. Reusing an old idempotency key after changing the JSON contract can serve structurally valid but semantically obsolete data. It also hides the new extraction's usage under the old job, which defeats per-tenant accounting.

## When should you choose a simpler pattern?

This design is not suitable when extraction is a disposable, single-process batch with no external retry, no durable output, and no need to attribute usage. A deterministic file name plus atomic rename may be enough there. Do not build leases and fencing tokens merely to protect a local scratch file.

At the other extreme, stick with a workflow engine or an existing transactional job system when it already provides durable timers, attempt history, and fenced task ownership. Reimplementing those mechanics inside each webhook handler expands the pager surface. The catch is that the workflow ID must still come from the tenant-scoped business identity; adopting an orchestrator does not define that identity for you.

Also avoid automatic retries for permanent failures. Invalid source permissions, an unsupported schema version, or output that repeatedly fails the same validation rule needs a terminal state and operator-visible reason. Retry transport timeouts and expired leases with bounded backoff and jitter. Never let an unbounded loop spend a tenant's budget while producing no publishable record.

The decision rule stays plain: if a retry can cross a process boundary or create a durable side effect, establish the durable business key and atomic completion first. Add queue deduplication, locks, and scheduling policy afterward, where they can improve throughput without becoming the last line of defense.

## References

- https://owasp.org/www-project-top-10-for-large-language-model-applications/
- https://www.promptingguide.ai
- https://www.rfc-editor.org/rfc/rfc9110.html
- https://www.postgresql.org/docs/current/ddl-constraints.html
- https://redis.io/docs/latest/commands/set/
- https://docs.nats.io/using-nats/developer/develop_jetstream/model_deep_dive
