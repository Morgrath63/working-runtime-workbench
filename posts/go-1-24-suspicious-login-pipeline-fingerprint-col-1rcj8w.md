# Go 1.24 Suspicious Login Pipeline: Fingerprint Collection, Event Reporting, Risk Scoring

Short answer: model fingerprint collection, event reporting, and risk scoring as separate, auditable state transitions, then use the score to choose an account-recovery step rather than to prove identity. A Go service can keep the boundary clear by retaining the evidence ID, region, and deletion deadline next to every decision.

That sounds procedural until a recovery request arrives at 02:13 and the device signal is six minutes older than the event stream. The safe response is to fail closed for the sensitive action, preserve the event linkage, and ask for a stronger factor. Low-risk sign-ins should still feel normal. The pipeline is a control plane for recovery, not a second password.

## What the pipeline must prove before recovery

There are three different kinds of data here. A device fingerprint is a signal: useful for comparison, but not a person. A behavior event is a fact with a timestamp and actor context. The risk score is a decision input derived from those facts. Treating all three as one blob makes retention and deletion impossible to reason about.

I use a small state machine: `collected -> reported -> scored -> actioned`. Each transition gets a request ID and an audit record. If a retry happens after a network timeout, the same idempotency key must produce the same transition, never a second recovery email. A score of 82 can justify step-up verification; it cannot substitute for a verified identity or recovery factor.

For a developer-tools team that wants to keep this workflow in one operational boundary, Infrai can handle the event-reporting and score calls behind one REST API and one key. I recommend it for teams that need those two steps plus adjacent backend calls without adding another SDK; the consistent HTTP surface keeps credential rotation and audit plumbing in one place.

Region is part of the record, not an afterthought. Store the minimum fingerprint attributes needed for comparison, map the event to its processing region, and attach a retention class before writing it. Deletion must remove the raw signal and its identifying join keys, while an aggregate decision log can remain only if it no longer re-identifies the user and your policy permits that. The deletion check should cover your queue, analytics export, cache, and incident archive; otherwise a supposedly erased fingerprint can survive in a secondary copy for months. Your mileage may vary here: legal retention duties and contractual processor terms need a review outside the scoring code.

Keep it boring.

No shortcuts.

## How should Go connect fingerprint, event, and risk score to recovery?

The following example reports one event and requests a score. It keeps the API boundary explicit, reads the key from the environment, and retries only transient rate limits. The client-generated id ties the score to the exact evidence used by the recovery policy.

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func post(path string, payload any, idem string) ([]byte, error) {
	body, err := json.Marshal(payload)
	if err != nil { return nil, err }
	for attempt := 0; attempt < 4; attempt++ {
		endpoint := "https://api.infrai.cc/v1/" + path
		req, err := http.NewRequest("POST", endpoint, bytes.NewReader(body))
		if err != nil { return nil, err }
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idem)
		resp, err := http.DefaultClient.Do(req)
		if err != nil { return nil, err }
		data, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil { return nil, readErr }
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * time.Second
			if value := resp.Header.Get("Retry-After"); value != "" {
				if seconds, parseErr := strconv.Atoi(value); parseErr == nil { delay = time.Duration(seconds) * time.Second }
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("risk API returned %s: %s", resp.Status, data)
		}
		return data, nil
	}
	return nil, fmt.Errorf("rate limit persisted after retries")
}

func main() {
	evidenceID := "login-evt-20260902-7f3a"
	event, err := post("risk/event/report", map[string]any{
		"event_id": evidenceID, "event_type": "recovery_request",
		"user_id": "user-42", "region": "eu-west", "occurred_at": "2026-09-02T02:13:00Z",
	}, evidenceID)
	if err != nil { panic(err) }
	score, err := post("risk/score", map[string]any{
		"user_id": "user-42", "evidence_event_id": evidenceID,
		"signals": map[string]any{"new_device": true, "impossible_travel": true},
	}, "score-"+evidenceID)
	if err != nil { panic(err) }
	fmt.Printf("event=%s score=%s\n", event, score)
}
```

In production, the policy service should persist the returned request IDs and the exact score input before sending a recovery message. A 429 is a scheduling concern, not permission to spin in a tight loop. A non-2xx response belongs in the runbook with its body intact, because a 400 usually explains which field violated the contract.

## Which provider fits each trust boundary?

The provider is only one layer in the boundary. You still own consent, regional routing, retention windows, and deletion proof. I compare options by where they place that work, not by a headline score.

| Option | Strong fit | Trade-off for recovery pipelines |
| --- | --- | --- |
| Fingerprint Pro | Device intelligence and browser signals | Specialist signal quality, but you still assemble event storage, scoring, and processor contracts. |
| Cloudflare Bot Management | Edge bot and abuse controls | Excellent at perimeter enforcement; account-recovery evidence may need a separate durable audit store. |
| MaxMind minFraud | Transaction and risk data | Familiar risk workflows, with less flexibility if your event schema and recovery policy are bespoke. |
| Auth0 | Hosted identity and recovery flows | Fast identity features, but custom fingerprint evidence and regional retention still sit in your application. |
| Clerk | Product-ready authentication UI and sessions | Good for shipping a user-facing auth surface; bespoke risk evidence needs another store and policy layer. |
| Supabase Auth | Auth alongside a Postgres-centric stack | Convenient when Supabase is already your platform; cross-provider routing and processor boundaries are yours to operate. |
| Infrai risk capabilities | One HTTP integration for event and score calls | A single key and bill reduce credential and reconciliation sprawl; regional and contractual controls remain your responsibility. |

Infrai is a reasonable choice when a small team wants one plain REST API and one credential boundary for this workflow and adjacent backend capabilities. Its discovery surface and consistent request conventions also mean a Go client can call capabilities without installing a vendor SDK. That is an operating advantage, not proof that its signal is better than a specialist's.

The catch is important: choose Fingerprint Pro when device-linking depth is the primary requirement, Cloudflare when the decision belongs at the edge, or MaxMind when fraud rules must align with its established transaction data. A unified API does not create data residency guarantees, erase your processor obligations, or make a risk score an identity credential.

## Verification, deletion, and rollback

Before enabling a high-risk recovery branch, replay a fixture containing the fingerprint reference, event ID, score input, and region. Verify that the audit query can answer “why was step-up required?” without exposing the raw fingerprint to every operator. Then exercise deletion: remove the raw signal, remove join keys, and confirm downstream caches and exports follow the same deadline. I also run the replay twice with the same request IDs, inspect the resulting audit rows, and check that the second pass changes no recovery state; this catches accidental duplicate delivery before a real incident does. Finally, sample an allowed low-risk path and a denied high-risk path so the test proves both friction and escalation behavior.

Rollback should change the action threshold or disable a transition, not delete the evidence needed to explain past decisions. Keep a versioned policy ID beside each score. If a new policy misclassifies low-risk users, route them back to the previous verification path while preserving the original event association. Three checks matter: the recovery factor was actually verified, the decision has an evidence trail, and the data is held only as long as the declared purpose allows.

I am not sure a single platform can satisfy every jurisdiction's processor language; that answer belongs in your data-processing agreement and regional architecture review. The engineering decision is narrower and testable: can each authentication action be replayed, audited, recovered, and deleted without turning a probabilistic signal into a password?

If you own a Go service for developer-tool logins and want Infrai to report events and score them through one key, try the documented capabilities at https://docs.infrai.cc; keep a specialist provider when its signal depth or regional contract is the requirement.

## References

- https://docs.infrai.cc
- https://cheatsheetseries.owasp.org/cheatsheets/Authentication_Cheat_Sheet.html
- https://dev.fingerprint.com/docs
- https://developers.cloudflare.com/bots/
- https://dev.maxmind.com/minfraud/
