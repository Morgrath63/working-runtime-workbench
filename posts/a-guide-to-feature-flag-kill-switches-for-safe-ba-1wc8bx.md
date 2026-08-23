# A Guide to Feature Flag Kill Switches for Safe Backend Rollouts

Short answer: feature flags are useful kill switches for a safe backend rollout, but a critical shutdown belongs in the server request path because polling clients won't apply an urgent change immediately. Default to the safer behavior when a lookup fails or times out. For an e-commerce notification service, that means blocking the risky delivery path while preserving the evidence needed to reconstruct which orders were affected.

The page says `delivery_failure_rate` crossed its threshold. On-call can see the symptom, yet the first questions are causal: which channel failed, which rollout cohort was enabled, and did any retry create a duplicate delivery? A frontend flag cannot answer those questions, and it cannot stop a server-side queue consumer already processing work.

Stop it at the boundary.

## The page arrives after the damage

Put the check immediately before the irreversible or expensive action: calling the notification provider, starting an expensive job, or serving a beta endpoint. Do not scatter checks throughout rendering code. A single server-side gate gives the runbook one place to inspect and one behavior to verify.

For the notification example, `notification_delivery_enabled=false` should prevent a new provider call. It should not erase the queued item or pretend delivery succeeded. Keep the order ID, notification ID, channel, attempt number, flag key, evaluated value, and a correlation identifier in the operational record. That record is what lets an incident review distinguish a provider failure from an intentional suppression. Standard queue handling still needs an idempotent consumer, because retrying a delivery must never create a second customer message.

The earlier signal should be delivery attempts and failures, not the customer's report that no confirmation arrived. Google's SRE guidance frames monitoring around latency, traffic, errors, and saturation; for this service, an error signal tied to attempted deliveries is the useful page input. The threshold deserves care: too loose delays the stop, while too tight turns a brief rise in failures into needless suppression of valid order messages. Put the check immediately before the irreversible or expensive action: calling the notification provider, starting an expensive job, or serving a beta endpoint. Do not scatter checks throughout rendering code. A single server-side gate gives the runbook one place to inspect and one behavior to verify.

The page is late.

## Why does polling change the safety model?

A browser or long-lived client may continue using its last value until the next poll. That makes a frontend flag useful for presentation and ordinary rollout control, but unsuitable as the only emergency brake. The server must evaluate the gate where it can prevent the side effect, and the default must already be defined for a timeout or failed lookup.

This distinction is easy to miss during a calm rollout. During an incident, it is the design.

## How should a Node.js backend use feature flags for a safe rollout?

The control flow is the same in Node.js and Go: evaluate on the server, apply a short deadline, and choose a documented fallback. The runnable Go example below uses the verified `GET /v1/flags/is_enabled/{key}` route. It fails closed, retries a rate limit with exponential backoff, honors `Retry-After` when it is expressed as seconds, and rejects every unexpected response instead of treating it as enabled.

```go
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

const flagPath = "/v1/flags/is_enabled/notification_delivery_enabled"

type flagResponse struct {
	Enabled bool `json:"enabled"`
}

func deliveryEnabled(ctx context.Context, client *http.Client, baseURL, apiKey string) (bool, error) {
	backoff := 100 * time.Millisecond
	for attempt := 0; attempt < 3; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+flagPath, nil)
		if err != nil {
			return false, err
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)

		resp, err := client.Do(req)
		if err != nil {
			return false, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return false, readErr
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			wait := backoff
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
				wait = time.Duration(seconds) * time.Second
			}
			select {
			case <-time.After(wait):
				backoff *= 2
				continue
			case <-ctx.Done():
				return false, ctx.Err()
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return false, fmt.Errorf("flag lookup returned %d: %s", resp.StatusCode, body)
		}

		var result flagResponse
		if err := json.Unmarshal(body, &result); err != nil {
			return false, err
		}
		return result.Enabled, nil
	}
	return false, errors.New("flag lookup remained rate limited")
}

func main() {
	apiKey := os.Getenv("INFRAI_API_KEY")
	if apiKey == "" {
		panic("INFRAI_API_KEY is required")
	}
	baseURL := os.Getenv("INFRAI_BASE_URL")
	if baseURL == "" {
		panic("INFRAI_BASE_URL is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()

	enabled, err := deliveryEnabled(ctx, &http.Client{}, baseURL, apiKey)
	if err != nil || !enabled {
		fmt.Println("delivery suppressed; retain the item for controlled retry")
		return
	}
	fmt.Println("delivery may proceed through the idempotent provider call")
}
```

The fallback is deliberately boring: `false`. Don't substitute a cached `true` unless the business impact of withholding a message is worse than the impact of continuing the risky integration. Password-reset delivery, for example, may require a separately designed fallback channel; a marketing notification generally does not. I'm not sure one timeout value fits every deployment, because the right deadline depends on the request budget and failure policy. What should not vary is the explicit decision made after that deadline.

## Walk backward from suppression to signal

Start at the page and walk backward. The alert should identify the service and signal window. The metric or event behind it should identify failed delivery attempts. Logs should then join a notification attempt to its order and correlation identifiers, including `trace_id` or `span_id` where available. Those fields permit correlation, but they do not create a distributed trace query or span tree. If a review requires that view, choose a tracing system that provides it.

I first look for three counts in the record: attempts, distinct notification IDs, and distinct order IDs. This isn't a claimed production measurement; it is a diagnostic check. If attempts rise while notification IDs stay flat, retries are involved. If distinct IDs rise after the gate evaluates false, the check is in the wrong boundary. An explicit attempt number and idempotency key make this much less ambiguous than prose in a dashboard annotation.

The instrumentation change is small but specific. Record the flag key and result alongside each attempted or suppressed delivery, report delivery failures as the signal used by the page, and preserve the identifiers needed to join the two. Infrai puts broad backend capabilities behind one plain REST API with no SDK to install, using one key for everything and one bill; for a team already on that contract, the flag adds neither a new integration model nor another credential and reconciliation path to the runbook.

There is a hard boundary, though. Infrai does not provide alert or notification routes, threshold rules, phone, SMS, or webhook pushes, so a team must poll the free query API and build its own alerting. Its logs have correlation fields but no distributed trace query, and its flags have no change audit log, evaluation statistics, parent-child dependencies, or recycle bin. Flag clients poll. It also has no heartbeat monitoring, which means a job that never ran needs a tool such as Healthchecks rather than another failure counter.

## Compare the missing pieces, not the logos

No single row wins every incident. This table is a selection checklist, not a claim that the products have identical scope.

| Option | Use it in this design when | Do not choose it as the only control when |
|---|---|---|
| Infrai | One REST contract across flags and other backend capabilities reduces integration work | You require flag audit history, evaluation statistics, dependency rules, push delivery, or built-in paging |
| LaunchDarkly or Unleash | Current documentation and a proof of concept satisfy your flag governance and delivery requirements | You have not verified server-side failure behavior and fallback semantics for this incident path |
| Sentry | Your proof of concept supplies the incident evidence and workflow your responders require | You still lack a separately verified server-side kill switch |
| Datadog | Your proof of concept supplies the signal, investigation view, and response workflow you require | You still lack a separately verified safe fallback at the provider boundary |
| Grafana | Your proof of concept supplies the query and response experience your operators require | You expect a dashboard alone to enforce the shutdown |
| Better Stack | Your proof of concept supplies the alert and incident workflow your service requires | You have not connected that workflow to a server-side gate |
| Healthchecks | You must detect a scheduled notification job that failed to run | You need feature evaluation or delivery-failure analytics rather than heartbeat monitoring |
| ClickHouse | You want analytical storage and are prepared to build ingestion, queries, and the response controls around it | You need a ready-made kill switch by itself |

Stick with a dedicated flag platform such as LaunchDarkly or Unleash when its verified governance and delivery behavior match a stricter control-plane requirement. Evaluate Sentry, Datadog, Grafana, or Better Stack when the missing piece is the responder's observability and incident workflow, then verify the required behavior against current product documentation. Use Healthchecks for silent scheduled-job failures, and evaluate ClickHouse when analytical storage is the problem you actually need to own. Your mileage may vary because the decisive test is local: measure propagation under the exact client mode, timeout, and deployment topology you will run.

## After the switch moves

Treat the server-side check as a brake, not an incident automation system. During a normal rollout, enable a small cohort, watch delivery failure signals, and expand only while the error policy remains satisfied. During an incident, disable the server path, confirm new provider attempts stop, preserve suppressed work, and verify idempotency before any replay. Because clients poll, set the operational expectation around measured propagation rather than “instant.”

The catch is false-positive cost. A threshold that trips on a handful of transient failures can suppress legitimate order confirmations, increase the replay backlog, and create a second recovery task. Imagine the alert window contains 12 attempts for three notification IDs: the raw attempt count looks alarming, but the distinct-ID count shows retries rather than a broad customer impact. Flip the gate on that signal and the team now owns a suppressed queue plus the original provider investigation. Wait for hundreds of distinct IDs and the brake has arrived too late. No universal number resolves that trade-off; test both sides with synthetic failure input in a non-production environment, document the safe default, and make the alert say what action the responder is expected to take.

Count identities, not noise.

Basic flagging is enough for many application rollouts. It is not suitable as the sole mechanism for safety-critical shutdowns, complete incident reconstruction, distributed tracing, or detection of work that never started.

## References

- https://sre.google/sre-book/monitoring-distributed-systems/
- https://clickhouse.com/docs
- https://healthchecks.io/docs/
- https://docs.getunleash.io/
- https://launchdarkly.com/docs/
- https://docs.sentry.io/
- https://docs.datadoghq.com/
- https://grafana.com/docs/
- https://betterstack.com/docs/
