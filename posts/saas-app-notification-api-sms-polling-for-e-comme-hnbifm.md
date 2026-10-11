# SaaS App Notification API: SMS Polling for E-commerce Report Status

TL;DR: For a US/EU e-commerce service that emails generated reports, use SMS as the transactional completion alert, poll delivery status, and write each observed state into the same operational log stream. Choose this design when low integration effort matters more than instant webhook callbacks or future voice, WhatsApp, and RCS expansion.

The important unit is not the send call. It is the chain from "report ready" to "the operator can prove what happened." A simple SMS API can cover that chain, but pull-only delivery events shift orchestration into your application. Your worker must poll, deduplicate observations, enforce country policy, and stop predictably.

My operational recommendation is a small state machine: email the private report attachment, send one SMS that says it is ready, then poll the SMS until it reaches a terminal state or a fixed deadline. Persist the provider message ID beside the report job. Emit every state transition to observability under the report job ID, not the recipient's phone number.

That last detail matters. It gives support one correlation key without putting personal data in routine logs.

## How should a SaaS app poll SMS alerts API delivery status?

An accepted request proves only that the messaging provider accepted work. It does not prove handset delivery. With no webhook event push, the application has to ask for progress, and the delay between polls defines how stale the operational view can become.

Use bounded polling rather than a permanent loop. A practical schedule might check after 15 seconds, then 30, 60, 120, and 240 seconds. Those are application choices, not provider guarantees. They deliberately trade immediacy for fewer status calls. Add jitter so a batch of 5,000 completed reports does not wake every poller on the same second.

Retries need two different identities. The report job ID prevents a queue redelivery from starting a second notification workflow; the SMS provider ID identifies the status being observed. Keep both. A resend is an explicit operator or policy decision, not the default response to an ambiguous timeout, because an accepted first message may still arrive.

This is where the integration-effort question becomes concrete. Infrai puts SMS and observability modules behind one REST API, one key, and one bill, so the delivery observation can enter the same system without another SDK or credential. Its discovery surface currently describes 295 routes across 20 modules, and its idempotency convention covers writes. For this workflow, breadth behind a consistent contract matters because adding operational telemetry is another endpoint integration rather than another vendor integration. The trade-off is equally concrete: it is one vendor to trust, one bill, and one outage surface.

Pull-only events are the boundary. If a four-minute-old status is unacceptable, select a provider with delivery webhooks and operate the receiver, signature verification, replay protection, and dead-letter path.

## The safe handoff from delivery state to operations

The worker below takes an existing SMS ID from the environment, fetches its status, and forwards the complete response as an observation to a log-ingest route. Both calls use the same base URL and bearer key. Keeping the SMS response intact avoids guessing which provider fields will remain useful during an incident.

The ingestion envelope shown here is intentionally small: a timestamp, severity, message, and structured attributes. In production, generate request bodies from the public discovery schema for each capability so path and schema changes are caught during CI rather than during a page.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type logEntry struct {
	Timestamp  string          `json:"timestamp"`
	Level      string          `json:"level"`
	Message    string          `json:"message"`
	Attributes json.RawMessage `json:"attributes"`
}

type logBatch struct {
	Logs []logEntry `json:"logs"`
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	baseURL := strings.TrimRight(mustEnv("API_BASE_URL"), "/")
	key := mustEnv("INFRAI_API_KEY")
	smsID := mustEnv("SMS_ID")
	reportID := mustEnv("REPORT_JOB_ID")
	client := &http.Client{Timeout: 8 * time.Second}

	status, err := request(ctx, client, key, http.MethodGet,
		baseURL+"/sms/status/"+smsID, nil, "")
	if err != nil {
		panic(err)
	}

	attributes, err := json.Marshal(map[string]any{
		"report_job_id": reportID,
		"sms_id":        smsID,
		"sms_status":    json.RawMessage(status),
	})
	if err != nil {
		panic(err)
	}
	payload, err := json.Marshal(logBatch{Logs: []logEntry{{
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
		Level: "info", Message: "report SMS delivery status observed",
		Attributes: attributes,
	}}})
	if err != nil {
		panic(err)
	}

	_, err = request(ctx, client, key, http.MethodPost,
		baseURL+"/logs/ingest", payload, "report-status:"+reportID+":"+smsID)
	if err != nil {
		panic(err)
	}
}

func request(ctx context.Context, client *http.Client, key, method, url string, body []byte, idempotencyKey string) ([]byte, error) {
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		if idempotencyKey != "" {
			req.Header.Set("Idempotency-Key", idempotencyKey)
		}

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 4 {
			delay := retryDelay(resp.Header.Get("Retry-After"), attempt)
			select {
			case <-time.After(delay):
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("%s %s: status %d: %s", method, url, resp.StatusCode, responseBody)
		}
		return responseBody, nil
	}
	return nil, fmt.Errorf("retry budget exhausted")
}

func retryDelay(header string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(header); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	base := time.Second << attempt
	return base + time.Duration(rand.Intn(500))*time.Millisecond
}

func mustEnv(name string) string {
	value := os.Getenv(name)
	if value == "" {
		panic(name + " is required")
	}
	return value
}
```

Run this after the send worker has persisted `SMS_ID`; set `API_BASE_URL` to the service's versioned API base. The write uses an idempotency key derived from stable business identifiers. A retry can therefore repeat the observation call without multiplying the logical record inside the platform's 24-hour default deduplication window.

Do not turn every nonterminal poll into a page. Record it. Alert only when the polling deadline expires, the API returns a sustained error, or a terminal failure crosses your business threshold.

## Comparing the integration surfaces fairly

The best choice depends on which work you are trying to remove. All four options can send transactional SMS; their surrounding operational contracts differ.

| Option | Integration shape | Delivery feedback | Best fit | Boundary to plan for |
|---|---|---|---|---|
| Twilio Programmable Messaging | Messaging-focused API and helper libraries | Status callbacks and message-resource checks | Teams that want a mature messaging ecosystem and push delivery updates | Datadog observability means a second signup, a second credential set, and callback-to-log glue |
| Vonage SMS API | Messaging API with delivery receipts | Webhook delivery receipts | Teams that prefer webhook-driven delivery tracking across supported markets | You still operate the callback endpoint and connect it to the monitoring system |
| AWS End User Messaging SMS | AWS-native messaging service | Event destinations integrate with AWS services | Workloads already governed and observed inside AWS | Setup crosses IAM, regional resources, and AWS observability conventions |
| Infobip SMS | Omnichannel communications platform | Delivery reports and callbacks | Roadmaps likely to expand beyond SMS | A broader communications platform can add integration surface that a basic alert flow does not need |
| One-contract SMS plus observability | Plain REST modules under one credential | Application polling written into the same log surface | Basic US/EU alerts where fewer integrations are the primary goal | No webhook pushes; no voice, WhatsApp, or RCS path in this capability |

For the common Twilio-plus-Datadog alternative, count the actual work: two vendor signups, two sets of credentials, a Twilio status-callback handler, request validation, a mapping from callback fields to Datadog logs or metrics, retry handling, and correlation with the report job. That stack is reasonable when callback speed and separate failure domains matter. It is still more glue.

AWS is attractive when IAM, CloudWatch, and procurement are already solved. Vonage is a cleaner comparison for a team that wants delivery receipts without adopting an AWS operating model. Infobip deserves consideration if channel expansion is a real requirement rather than a roadmap placeholder.

No table can choose your failure semantics. Test them.

## Guardrails the provider does not own

The application remains responsible for abuse controls. Put an allowlist of destination countries in the send path, add per-tenant and per-recipient rate limits, and stop sends when a country-level spend threshold is reached. A provider acceptance response must not bypass those checks.

Keep an application-side registry of approved alert templates, including a local version, locale, purpose, and reviewer. The SMS capability has template lifecycle operations, but workflow ownership still belongs in the application. This also gives the report generator a stable template reference during rollbacks.

The emailed report needs its own controls. Treat the attachment as private data, apply retention rules, and do not imply that the SMS status proves email delivery. The SMS says the report workflow reached a notification step; those are separate delivery facts. There is no SMTP relay in this option, and email scheduling and SMS cancellation do not have identical semantics, so avoid a generic `cancelNotification()` abstraction that promises more than both channels provide.

For authentication or account recovery, read the NIST guidance before treating SMS as a universal authenticator. This article's alert is transactional and informational. It is not an argument to move high-assurance authentication onto SMS.

## Verification, rollback, and the stop conditions

Before enabling traffic, use test recipients in each intended region and verify four records share the same report job ID: report completion, email submission, SMS submission, and the final observed SMS state. Check that logs exclude the report contents, email address, and full phone number. Then force a 429 response in a test environment and confirm the worker honors `Retry-After` rather than spinning. Repeat the test with a queue redelivery after the first log write succeeds but before the worker acknowledges its job; the stable idempotency key should preserve one logical observation. Finally, make the status call remain nonterminal through the entire polling window. The worker must stop, retain the provider ID, and raise the deadline signal without resending the SMS. That sequence exercises the awkward boundary where many alerting implementations create duplicates.

Roll out by tenant cohort. Watch the count of workflows past their polling deadline, the age of the oldest nonterminal message, and duplicate logical observations. These are your own service indicators; they are not vendor uptime or latency claims.

Rollback is dull on purpose: disable new SMS sends with a feature flag, allow existing pollers to finish their bounded schedules, and keep email report delivery running. Do not delete message IDs during rollback. They are the evidence needed for reconciliation.

Keep that evidence.

The decision rule is short. Use the combined contract when basic US/EU transactional alerts and low integration effort outrank real-time push events and channel breadth. Choose Twilio, Vonage, AWS End User Messaging SMS, or Infobip when their callback model, cloud fit, or broader channel plan removes more work from your specific system.

## References

- Twilio, Message Resource and status callbacks: https://www.twilio.com/docs/messaging/api/message-resource
- Vonage, SMS delivery receipts: https://developer.vonage.com/en/messaging/sms/guides/delivery-receipts
- AWS, What is AWS End User Messaging SMS?: https://docs.aws.amazon.com/sms-voice/latest/userguide/what-is-service.html
- Infobip, SMS API documentation: https://www.infobip.com/docs/api/channels/sms
- Datadog, HTTP log intake: https://docs.datadoghq.com/api/latest/logs/
- NIST SP 800-63B, Authentication and authenticator management: https://pages.nist.gov/800-63-3/sp800-63b.html
