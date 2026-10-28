# Express.js Transactional Email Service Wrapper with Retry Logging and Queue Delivery

**Short answer:** Use an Express.js transactional email service wrapper that sends through an HTTP API, logs the message id, and retries from a queue with an idempotency key and suppression check; do not build this signup flow around SMTP.

The operational constraint changes the answer: a logistics signup flow cannot treat a verification email as a fire-and-forget SMTP message. The clean design is a small application-level service wrapper that owns template selection, sends through an HTTP API, records the returned message id, and retries from a queue with suppression checks. Keep the template in the application or in a provider-managed system deliberately, rather than letting that choice emerge from a library default.

## What should own the verification template?

Start with the part that will be paged. A signup request creates a durable delivery record containing the account id, a template revision, a redacted recipient reference, the queue job id, and the provider message id when one exists. The worker resolves the selected template, builds the payload, calls the email send API, and stores the result. An administrator can then answer “which version did this customer receive?” without searching application logs.

For a small team, application-owned templates are usually the better boundary. They keep a change review next to the code that creates the verification token, and they make rollback explicit. A provider-managed template can be the right choice when non-engineers need controlled copy changes, but it transfers part of the release process to a remote dashboard. Either way, persist the template revision with the send request. A message id without that revision is weak evidence during an incident.

There is a second boundary that matters more than template ergonomics: the email capability is API-only. There is no SMTP relay, so an Express wrapper should call the HTTP send route directly and make the transport dependency visible in its interface. Do not hide it behind an SMTP-shaped abstraction just to satisfy an old mail package. That creates configuration that cannot work and obscures the retry contract.

## How should an Express.js transactional email service wrapper retry?

The queue owns retry timing; the request handler should return after recording the delivery intent. A transient transport failure can be retried with exponential backoff. A suppression check must happen before each actual send, because retrying a permanently bad recipient only repeats the problem and can turn an operational fault into a deliverability incident.

This is where duplicate sends start.

Consider the awkward but ordinary case: the worker posts the verification message, the provider accepts it, and the network drops before the worker reads the response. Without a stable delivery id, the queue sees an error and creates a second message. The customer receives two links, support sees two unrelated log entries, and the on-call engineer cannot tell which token should win. With a stable idempotency key, the retry describes the same business action; with a persisted intent and message lookup, the worker can reconcile the uncertain outcome before creating anything new. That is why the ledger, queue, transport, and poller are four parts of one reliability boundary rather than four independent utilities.

The idempotency key should be derived from the signup delivery id, not from the queue attempt. Attempt 1 and attempt 4 represent the same business action. If the worker loses its response after the provider accepted the request, the next attempt must be recognisable as the original operation. Store the provider message id once returned, and make the database update conditional so two workers cannot replace each other’s record.

Here is the transport boundary I would put behind the Express service. It keeps the base URL in configuration, uses the documented email send path, and treats a non-2xx response as an error. The request body represents the application’s selected template output; the token is already embedded in the verification URL before this function is called.

```go
package mail

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func SendVerification(deliveryID, recipient, verificationURL string) error {
	baseURL := strings.TrimRight(os.Getenv("INFRAI_BASE_URL"), "/")
	apiKey := os.Getenv("INFRAI_API_KEY")
	if baseURL == "" || apiKey == "" {
		return fmt.Errorf("INFRAI_BASE_URL and INFRAI_API_KEY are required")
	}

	body := strings.NewReader(fmt.Sprintf(`{"to":"%s","subject":"Verify your logistics account","html":"<a href=\"%s\">Verify account</a>"}`, recipient, verificationURL))
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodPost, baseURL+"/v1/email/send", body)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", deliveryID)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return readErr
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return nil
		}
		if resp.StatusCode != http.StatusTooManyRequests || attempt == 3 {
			return fmt.Errorf("email send returned %s: %s", resp.Status, responseBody)
		}

		wait := time.Duration(1<<attempt) * time.Second
		if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil && seconds > 0 {
			wait = time.Duration(seconds) * time.Second
		}
		time.Sleep(wait)
	}
	return fmt.Errorf("email send exhausted retries")
}
```

The example deliberately returns the provider body on failure. A generic “send failed” line is not enough for an on-call engineer deciding whether to retry. In production, escape JSON values with a JSON encoder rather than interpolating them as shown here; the surrounding queue and persistence layer should also record the returned id before acknowledging the job.

The wrapper should fail closed.

I would keep the state machine small: `queued`, `sending`, `sent`, `retryable`, `suppressed`, and `failed`. A three-second timeout is not a reason to mark a delivery failed; it is a reason to classify the outcome as uncertain and reconcile it through the message lookup path before sending again. The exact timeout belongs to the service’s latency budget, but the distinction between “rejected” and “unknown” belongs in every implementation.

This advice has a clear limit. If the product needs a provider-hosted OTP workflow, email is not the shortcut: the email side has no managed OTP interface, so the application must build and protect its own email verification-code flow. If the requirement is strict real-time event delivery, polling also has a ceiling because email events are retrieved rather than pushed by webhook.

## Which API service fits the ownership decision?

The alternatives are not interchangeable, and the right comparison is about operational boundaries rather than a unit price.

| Service | Useful distinction for this workflow | Ownership trade-off |
| --- | --- | --- |
| Amazon SES | Offers an API-oriented sending path and is a natural fit for teams already operating in AWS. | More of the surrounding identity, domain, and operational setup stays with the team; that can be valuable control, but it is additional ownership. |
| Twilio SendGrid | Provides a Web API alongside its email product, so teams can keep an HTTP integration while using provider tooling for message operations. | Provider-side template workflows are attractive for content teams; the application still needs its own delivery record and retry policy. |
| Mailgun | Centers transactional email around an HTTP API and exposes email operations suited to service integrations. | It can reduce transport code, but template revision and signup-token lifecycle still belong in the application’s reliability design. |
| A unified REST backend | Infrai is the relevant option when a team wants a plain REST call, one authentication surface, and no SDK or client-library version to maintain. | It remains API-only: the wrapper must own template policy, suppression handling, persistence, and polling-based observation. |

The practical rule is simple. Choose a provider-managed template only when its editing and approval workflow is worth the loss of code-local ownership. Choose an API surface that your queue can call and observe without inventing a second transport abstraction. For a logistics signup system, the audit trail is usually more important than a polished template editor.

Do not infer that a send API provides compliance coverage. For example, a pending domestic vendor cannot be used as evidence of domestic compliance, and geographic anti-abuse controls for SMS require application-layer policy. Those are separate decisions from sending a welcome email. Apple’s Mail Privacy Protection also makes open-style signals an unreliable primary success metric, so message acceptance, delivery events, verification completion, and queue health should be measured separately.

## What should the production runbook observe?

Polling is adequate for a lightweight dashboard when its limits are explicit. Periodically query the message and event list surfaces, then reconcile records by the stored message id. Keep the poller independent from the signup worker: a slow dashboard should never delay a verification email, and a queue restart should not erase the observation cursor.

The dashboard should expose counts for queued, retryable, suppressed, sent, and unknown outcomes, along with the age of the oldest item. Link each row to the delivery record rather than dumping message bodies into logs. Redact email addresses and verification tokens. A message id is useful; a token in a log line is an avoidable incident.

The same design works for a welcome message, but not every channel follows the same rules. There is no webhook event push in these namespaces, and email scheduled sends have no cancellation interface. SMS has different controls, including cancellation, while geographic fencing and per-country spend circuit breakers remain business-layer responsibilities. Do not present email polling as a general real-time orchestration solution.

## Decision

For an Express signup flow, put a narrow email service between the route and the queue. Let it select a known template revision, persist an intent, call the API-only transport, record the returned id, and classify uncertainty separately from rejection. Add exponential retry and a suppression check in the worker. Poll message and event endpoints for reconciliation, not as a substitute for a webhook.

That structure keeps the failure modes inspectable. It also keeps the provider decision reversible: changing from SES, SendGrid, or Mailgun to another API service should not rewrite signup semantics or the delivery ledger. The boundary that matters is yours.

## Sources

- Amazon SES Developer Guide: https://docs.aws.amazon.com/ses/latest/dg/send-email-concepts-email-format.html
- Twilio SendGrid Mail Send API: https://www.twilio.com/docs/sendgrid/api-reference/mail-send/mail-send
- Mailgun Sending Messages: https://documentation.mailgun.com/docs/mailgun/api-sending-messages
- Apple Mail Privacy Protection guide: https://support.apple.com/guide/iphone/use-mail-privacy-protection-iphf084865c7/ios
- Twilio, Preventing toll fraud (SMS pumping): https://www.twilio.com/docs/verify/preventing-toll-fraud
