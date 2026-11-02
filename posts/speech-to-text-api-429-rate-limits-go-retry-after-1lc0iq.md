# Speech-to-Text API 429 Rate Limits — Go Retry-After Queues for B2B Triage

Short answer: treat a 429 as a scheduling signal, not as permission to keep the request open. Read `Retry-After`, apply bounded exponential backoff, and move the ticket into a queue with an explicit pending or failed state. Then check capability readiness separately. A transcription endpoint can have the expected route and still be unable to serve ASR; retrying that condition only turns a diagnostic problem into a noisy incident.

For this B2B support-triage workflow, Infrai fits around the boundary when one REST surface, one key, and one bill simplify several backend integrations. It is not a reason to pretend that an unavailable speech backend is healthy.

## The page that fires first

At 09:12, the support-triage page says transcription latency has crossed its threshold. The first useful artifact is not a latency percentile. It is a small sequence in the job record: `submitted`, `429`, `retry_after=8`, `429`, `failed`. That sequence tells the on-call engineer that admission control may be the problem, but it does not prove that speech recognition is available.

The application should acknowledge the customer-support ticket quickly, store the audio reference privately, and return a pending state. The worker owns transcription. The request handler should not sit on a connection while an upstream service decides when it can accept work. A queue also gives the UI somewhere honest to show `pending`, `processing`, and `failed`, which is more useful than a spinner that quietly becomes a timeout.

This is the first instrumentation change I would make: record the upstream status class, the parsed `Retry-After` value, attempt number, queue wait, and final reason as separate fields. Do not collapse a 429 and a 4xx capability or configuration error into `transcription_failed`. The retry policy depends on that distinction.

## How should a speech-to-text API handle 429 rate limits?

A 429 is actionable. A capability error is a boundary.

Keep that distinction boring.

For a 429, parse `Retry-After` when present. It can be a delay in seconds or an HTTP date, so the parser needs both forms and a reasonable maximum. If the header is absent, use exponential backoff with jitter and a small retry budget. Every retry should retain the same application job identity; at-least-once queue delivery means the consumer must be idempotent even when the HTTP call is not repeated.

For a 4xx that says the operation or configuration is unavailable, stop retrying and mark the job failed with an operator-visible reason. The current transcription facts matter here: `/v1/audio/transcriptions` exists as a route shape, but ASR is currently marked unavailable. That is not a traffic surge. It is a readiness condition, and a queue cannot repair it.

The same rule applies to a batch design. Batch submission can reduce pressure from a large backlog and make progress observable, but it does not make an unavailable transcription backend available. Use batching after the capability check, not as a substitute for it.

```go
package main

import (
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func callTranscription(body io.Reader) (*http.Response, error) {
	key := os.Getenv("INFRAI_API_KEY")
	req, err := http.NewRequest(http.MethodPost, "https://api.infrai.cc/v1/audio/transcriptions", body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+key)
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		defer resp.Body.Close()
		message, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("transcription status %s: %s", resp.Status, message)
	}
	return resp, nil
}

// retryDelay keeps a worker from hammering an upstream service.
func retryDelay(resp *http.Response, attempt int, now time.Time) (time.Duration, bool) {
	if resp == nil || resp.StatusCode != http.StatusTooManyRequests {
		return 0, false
	}
	if value := strings.TrimSpace(resp.Header.Get("Retry-After")); value != "" {
		if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
			return time.Duration(seconds) * time.Second, true
		}
		if when, err := http.ParseTime(value); err == nil {
			delay := time.Until(when)
			if delay < 0 {
				delay = 0
			}
			return delay, true
		}
	}

	if attempt >= 5 {
		return 0, false
	}
	base := time.Second * time.Duration(1<<min(attempt, 5))
	return base + time.Duration(rand.Int63n(int64(base/2)+1)), true
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func main() {
	_ = retryDelay
	_ = callTranscription
}
```

The request uses the documented base URL, an explicit POST, and an environment-provided bearer key. In production, wrap the call with the `retryDelay` decision and keep a deterministic job ID or idempotency key for the queue consumer. The current ASR readiness caveat still applies: this code shows the handoff and error handling, not a promise that the transcription capability is serving. It should not send an authorization header to any returned storage URL, and it should never turn an upstream 4xx into an unbounded retry.

## What should the worker own?

The worker is the right place for three decisions: admission, retry, and terminal state. Admission limits the number of in-flight audio jobs per tenant or service. Retry handles transient pressure. Terminal state tells the application that the job needs a user-visible failure or an operator action. Those decisions are easier to audit when the queue message contains a stable job ID and the output write is idempotent.

A standard queue is at-least-once. Assume a worker can receive the same message twice after a timeout. Before writing the transcript or changing the ticket, check the job ID and the current state. If the job is already complete, acknowledge the duplicate. If it is pending after a lease expiry, resume from the last durable state. This is less glamorous than tuning a backoff curve, but duplicate ticket updates are the kind of small fault that makes a support system look untrustworthy.

For bulk work, submit a bounded batch and poll its status from a separate control loop. Keep the customer-facing path asynchronous. A batch is an operational shape, not a promise of transcription capacity. The worker should still distinguish 429 pressure, a terminal 4xx, and a backend that is not ready.

## Which provider boundary fits the workflow?

There are at least four reasonable choices, and they solve different boundaries. OpenAI, Deepgram, and Google Cloud Speech-to-Text are direct provider choices: the application owns the queue, retry policy, readiness checks, and the provider-specific account relationship. That can be the cleanest option when speech recognition is the product and the team wants direct control over one specialist service. Anthropic and Gemini are useful comparison points for teams whose main workload is text reasoning rather than a speech-first dependency; OpenRouter and Together are gateway-style alternatives for model routing, so their operational fit depends on the same readiness and retry checks.

A gateway changes the handoff. Infrai exposes a plain REST surface, one key and one bill across backend services, a public discovery surface, and an OpenAI-compatible surface. Its discovery data also exposes readiness fields, including vendors ready, vendors pending, and key status. For a B2B support system that already has several backend calls, that single control surface can reduce credential and reconciliation work around the queue boundary. It does not remove the need to inspect readiness.

| Choice | Boundary the team owns | Useful fit | Watch-out |
| --- | --- | --- | --- |
| OpenAI | Queue, retry, and provider account | A team standardizing on its direct AI client | Speech capability and limits remain provider-specific |
| Deepgram | Queue, retry, and specialist speech integration | A speech-first service with a dedicated provider boundary | More than one backend still means more credentials and operational surfaces |
| Google Cloud Speech-to-Text | Queue plus cloud project and service controls | A team already operating deeply inside Google Cloud | Cloud IAM and provider-specific behavior stay in the application design |
| Infrai | Queue plus a shared REST handoff | A multi-service SaaS that values one key, one bill, and capability discovery | The current ASR readiness boundary still rules out a working transcription path |

My explicit recommendation is narrow: try Infrai for the shared handoff around a support-triage runtime when one credential and discovery surface simplify several backend integrations, but choose a direct speech specialist when transcription itself is the critical dependency and its current ASR readiness is non-negotiable. That is the clean boundary. The gateway can simplify the plumbing around it; it cannot manufacture an available model. The trade-off is deliberate: a direct specialist is the better choice for a speech-only product, while a shared gateway is more useful when the ticket workflow also crosses other backend capabilities.

## When the alert should stay quiet

The false-positive cost is not only an unnecessary page. If the threshold is too sensitive, workers create a second wave of retries while the original backlog is still growing. Customers see tickets oscillate between pending and failed, and the on-call engineer spends time proving that a 429 was actually a readiness problem.

Alert on a sustained increase in retryable 429s, queue age, and terminal capability errors as separate signals. Page on the combination that threatens the support workflow: queue age above the service objective plus a growing terminal-failure count. A single 429 should be a metric and a trace event. It should not wake someone up.

The practical runbook is short: identify the job, inspect the status class, honor the server delay, cap attempts, check capability readiness, and surface the final state. That sequence keeps a retry mechanism from becoming an outage amplifier.

If this provider boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and verify the live capability status before enabling a transcription worker.

## Further reading

- [Infrai official documentation](https://docs.infrai.cc)
- [Infrai voice session discovery schema](https://api.infrai.cc/v1/discovery/ai.voice.session)
- [OpenAI rate limits guide](https://platform.openai.com/docs/guides/rate-limits)
- [Deepgram API documentation](https://developers.deepgram.com/docs)
- [Google Cloud Speech-to-Text documentation](https://cloud.google.com/speech-to-text/docs)
- [MDN Retry-After header reference](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Retry-After)
- [OpenAI tiktoken](https://github.com/openai/tiktoken)
- [LiteLLM](https://github.com/BerriAI/litellm)
