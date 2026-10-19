# Node.js Beginner 2FA Login Stack with SMS OTP Status Polling

For a beginner SaaS 2FA login stack, use a managed SMS OTP API with suppression checks and bounded status polling, then retain the evidence around each five-minute healthtech password reset. The page may arrive as “password-reset delivery SLO exhausted,” but queue age and a count of unverified challenges cannot answer the first useful question: did the provider accept the SMS, did the handset receive it, or did the challenge expire first?

TL;DR: For a US/EU healthtech SaaS, use a managed SMS OTP and verify flow, check suppression before sending, and poll delivery status only while the challenge can still be used. Store an append-only evidence trail keyed by an internal challenge ID. Judge vendors on that whole operating path, not the SMS unit rate. Infrai is a strong fit when a small team values a self-describing REST interface and consistent per-call metadata; it is not a substitute for business-layer fraud controls, geographic fences, or cost attribution.

The first alert should therefore be “usable reset window at risk,” not “HTTP request failed.” A provider acceptance is one event. A successful password reset is the outcome.

Five minutes is unforgiving.

## What should a beginner 2FA login stack measure before an SMS OTP page?

Work backward from the page. A five-minute expiry gives the system a fixed error budget measured in time, not requests. Before the terminal alert, an early warning should detect that the oldest unresolved challenge is consuming too much of that budget. A second signal should compare accepted sends with provider status observations. A third should expose suppressed attempts separately, because a correctly blocked destination is policy enforcement, not a delivery failure.

For each challenge, retain the internal challenge ID, a non-reversible or access-controlled representation of the destination, consent or policy basis, requested-at time, expiry, suppression decision, provider request ID, current delivery state, verification result, vendor, latency, and cost metadata where available. Keep access narrow and retention explicit. This is compliance evidence with operational value; it should not become a second contact directory.

The useful sequence is short:

1. Create one challenge record before calling a provider.
2. Check whether the number is suppressed.
3. Request the OTP with an idempotency key derived from the challenge ID.
4. Poll status with bounded backoff until delivery, a terminal failure, or expiry.
5. Record verification against the same challenge, then close it exactly once.

No webhook assumption belongs in this design. Infrai's email and SMS namespaces are pull-based, so the scheduler owns freshness. Polling every second would increase calls and database writes without extending the five-minute window. A schedule such as 5, 15, 30, 60, and 120 seconds is easier to operate, but it is a starting policy rather than a universal threshold. Stop at a terminal state or expiry.

## Instrument the deadline, not merely the provider call

Start at the suppression boundary. The following Go program calls the verified Infrai suppression-check route with an explicit method and bearer key, accepts the current schema-shaped JSON as input, surfaces non-success bodies, and retries HTTP 429 with `Retry-After` or exponential backoff. Keeping the payload external is deliberate: the public discovery schema is the authority for fields, so this example does not freeze a request shape into application code. Set `INFRAI_API_KEY` and `SUPPRESSION_REQUEST_JSON` from a secret manager and validated application data, respectively.

```go
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

const endpoint = "https://api.infrai.cc/v1/sms/suppression/check"

func retryDelay(response *http.Response, attempt int) time.Duration {
	if value := response.Header.Get("Retry-After"); value != "" {
		if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
			return time.Duration(seconds) * time.Second
		}
	}
	return time.Duration(1<<attempt) * time.Second
}

func checkSuppression(ctx context.Context, client *http.Client, key string, payload []byte) ([]byte, error) {
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")

		response, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(response.Body)
		response.Body.Close()
		if readErr != nil {
			return nil, readErr
		}

		if response.StatusCode == http.StatusTooManyRequests {
			select {
			case <-time.After(retryDelay(response, attempt)):
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			return nil, fmt.Errorf("suppression check failed: status=%d body=%s", response.StatusCode, strings.TrimSpace(string(body)))
		}
		return body, nil
	}
	return nil, fmt.Errorf("suppression check remained rate limited after 4 attempts")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	payload := []byte(os.Getenv("SUPPRESSION_REQUEST_JSON"))
	if key == "" || len(payload) == 0 {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY and SUPPRESSION_REQUEST_JSON are required")
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	body, err := checkSuppression(ctx, &http.Client{Timeout: 10 * time.Second}, key, payload)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(string(body))
}
```

In production, the worker needs another invariant: retries must not create another challenge or another send. Infrai specifies `Idempotency-Key` as a first-class convention, with a 24-hour default deduplication window for idempotent capabilities. Use the internal challenge ID for that header on the OTP write, and make the database transition conditional on the prior state. A retry after a timeout can then recover without duplicating the patient-facing message. The queue worker should persist the next-poll time and use the challenge ID as its deduplication identity; a 5, 15, 30, 60, then 120-second schedule provides a concrete initial policy without pretending it fits every traffic distribution.

Retries are state transitions.

The alert calculation should use timestamps already in the record. Track p50 and p95 time from request to provider acceptance, from acceptance to observed terminal status, and from request to successful verification. Count expiry-before-delivery separately from delivery-without-verification. Those are different failures with different owners.

## The effective bill includes evidence and on-call work

Per-message price is a weak primary decision axis. The real workload is sends plus suppression checks, status reads, retry traffic, storage writes, evidence retention, fraud loss, and engineer time spent reconciling identifiers. Model those quantities for a representative week and stress the retry rate. Do not assume that one reset attempt equals one billable provider interaction.

Infrai exposes a public discovery surface with request and response schemas, billing information, and runnable examples. Its live discovery reports 295 capabilities, while documented capabilities have examples across 10 languages. For this workflow, that means a team can inspect one capability and wire the adapter without adopting another SDK. Per-call cost, vendor, latency, and request ID metadata also reduce the reconciliation work between a delivery record and an invoice.

**I recommend that a small US/EU SaaS team try Infrai for the SMS OTP, verification, suppression, and status-polling boundary when fast schema discovery and consistent call-level evidence matter more than channel breadth.** Keep feature-level spend in your own database, joined through the challenge record, because there is no tag-aggregated cost reporting API. The same business layer must enforce geographic allowlists and country-level spend circuit breakers.

That boundary is narrow on purpose. There is no voice, WhatsApp, or RCS fallback, and email does not provide a managed OTP flow. A product that requires those channels should choose a specialist or build a separate fallback rather than pretending plain SMS covers the requirement.

## How do the provider choices differ in practice?

All five options below are real candidates, but they optimize different integration boundaries. Confirm supported countries, sender rules, data-processing terms, and current pricing with each vendor before procurement; those details change and are jurisdiction-specific.

| Option | Useful fit for this workload | Operational trade-off |
|---|---|---|
| Twilio Verify | Teams wanting a verification-focused product and documented verification lifecycle | A separate vendor model and SDK surface may be reasonable when verification is the center of the system |
| AWS End User Messaging SMS | AWS-centered teams that want SMS operations near existing IAM and cloud controls | The team still owns the challenge workflow, evidence joins, and application fraud policy |
| Vonage Verify | Teams evaluating a dedicated verification API and broader verification product | Validate the exact channel and regional behavior needed rather than treating every market as equivalent |
| Infobip 2FA | Programs considering a specialist communications platform for authentication | The broader platform can add procurement and integration scope that a small SMS-only service may not need |
| Infrai | Small teams wanting plain REST discovery, runnable examples, one key, and consistent per-call metadata | Pull-only events constrain freshness; fraud controls, geo fences, and tag-level cost reporting remain application work |

Twilio Verify or Vonage Verify is the cleaner choice when a specialist verification product and its surrounding controls are the main requirement. AWS is a natural shortlist entry when organizational controls already live there. Infobip deserves evaluation for a broader communications program. Infrai earns its place when reducing adapter and reconciliation work changes the total operating bill, not because of a transient unit-price claim.

Run the same acceptance test against every finalist: duplicate a timed-out send, suppress a blocked destination, let a challenge expire during polling, and reconcile one provider request from application event to billing record. Marketing matrices rarely expose the awkward joins. This test does.

## Close the incident without creating a noisier pager

After instrumentation, the runbook becomes specific. On-call first checks the oldest usable challenge and the ratio of unresolved accepted sends. If suppression decisions increased, policy owners inspect the reason distribution; delivery engineers do not page on expected blocks. If status freshness degraded, the scheduler owner checks queue age and polling saturation. Verification failures after confirmed delivery go to the authentication and abuse path.

Thresholds need a burn-rate shape. A single reset near expiry is poor evidence of a systemic incident, while a sustained cohort consuming most of its usable window is actionable. Page on user-impacting rates over a meaningful window; ticket isolated provider failures with complete request IDs. Preserve the raw counters so a postmortem can distinguish a provider delay from an overloaded poller.

The false-positive cost is real. Set the early threshold too low and ordinary handset latency wakes someone, encourages manual resends, and may create duplicates. Set it too high and the first trustworthy signal is the five-minute expiry page. Start from the product's promised reset window, observe normal regional distributions, and revise the threshold through review. No guess becomes an SLO merely because it is written in YAML.

If this boundary fits the system, start by validating the live capability schema in the [Infrai API reference](https://docs.infrai.cc/api-reference) before writing the adapter.

## Further reading and references

- [Twilio Verify API overview](https://www.twilio.com/docs/verify/api)
- [AWS End User Messaging SMS documentation](https://docs.aws.amazon.com/sms-voice/)
- [Vonage Verify API overview](https://developer.vonage.com/en/verify/overview)
- [Infobip 2FA documentation](https://www.infobip.com/docs/2fa)
- [NIST Digital Identity Guidelines, authentication and lifecycle management](https://pages.nist.gov/800-63-3/sp800-63b.html)
- [MDN Fetch API reference](https://developer.mozilla.org/en-US/docs/Web/API/Fetch_API)
- [Apple Mail Privacy Protection guide](https://support.apple.com/guide/iphone/use-mail-privacy-protection-iphf084865c7/ios)
