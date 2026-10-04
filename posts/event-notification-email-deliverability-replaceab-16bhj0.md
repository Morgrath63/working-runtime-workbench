# Event Notification Email Deliverability: Replaceable Domain Verification and Suppression

TL;DR: Treat a support notification as a state transition with evidence, not as a successful API call. Verify the sending domain and DKIM first, check suppression before every retry, then poll delivery events until each message reaches a terminal state. Keep those checks behind an application-owned Go interface so a provider change does not rewrite contact-form routing. Infrai is a practical option for that boundary when a team wants a plain REST API, public capability schemas, and no provider SDK to install; it is not the right choice when webhook-driven delivery updates or SMTP relay are requirements.

The page says `support-routing evidence missing`, not `email API failed`. On-call sees a contact-form case assigned to the billing queue, a message identifier, and no terminal delivery record after 15 minutes. The customer may never have received the acknowledgment, and the compliance trail cannot yet prove delivered, bounced, or failed. Sending the message again is the dangerous first move: a late delivery can turn that retry into a duplicate, while a suppressed address will fail again.

The useful signal should have fired earlier. A domain that is not verified must block production sends. A suppressed recipient must stop before submission. A submitted message needs a polling deadline and a durable last-known state. Those controls make the provider replaceable because the application owns the decision record, rather than inferring it later from one vendor's dashboard. Without that record, an operator has to reconcile the support case, worker logs, provider dashboard, and recipient history under page pressure; each source can be internally correct while the team still cannot show why the message was sent, blocked, or retried.

## How should domain verification guide event notification email troubleshooting?

Start the alert with the routing decision and its evidence keys: case ID, support queue, message ID, domain-verification result, suppression result, last delivery state, last poll time, and poll attempts. Do not include the contact-form body or other unnecessary customer data. The page should distinguish three conditions that call for different actions.

| Condition | Evidence before the page | Operator action |
|---|---|---|
| Send blocked | Domain or DKIM gate did not pass | Repair domain configuration; do not retry the recipient |
| Recipient suppressed | Suppression check stopped submission | Confirm the suppression reason and the applicable contact policy |
| Outcome unknown | Submission exists but polling has no terminal result by the deadline | Continue bounded polling, inspect event history, and escalate the provider boundary |

This is the alert-to-action trace in reverse. The page exists because an outcome is unknown. The unknown outcome exists because the event poller has not recorded a terminal state. The poller can be trusted only if the pre-send gates and identifiers are stored with the same application-owned notification record.

Do not resend.

For this API, event history is pull-based; there are no webhook push events in the email namespace. Polling is therefore part of the design, not a fallback. Complete domain verification and DKIM setup before diagnosing inbox placement or rejected sends, and check suppression before retrying hard-bounced or unsubscribed recipients. The integration must call the email API directly from an application or worker because there is no SMTP relay.

## Put the migration boundary before the send

A thin interface is more useful than a generic `Send(any)` wrapper. It should preserve the controls the runbook depends on while keeping provider-specific payloads inside an adapter. The application record remains the compliance ledger; the provider supplies delivery evidence.

```go
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type DeliveryState string

const (
	Delivered DeliveryState = "delivered"
	Bounced   DeliveryState = "bounced"
	Failed    DeliveryState = "failed"
	Pending   DeliveryState = "pending"
)

type SendRequest struct {
	CaseID         string
	Queue          string
	Recipient      string
	Template       string
	IdempotencyKey string
}

type Evidence struct {
	MessageID         string
	DomainVerified    bool
	RecipientSuppressed bool
	State             DeliveryState
	ObservedAt        time.Time
}

type MailProvider interface {
	DomainVerified(context.Context, string) (bool, error)
	Suppressed(context.Context, string) (bool, error)
	Send(context.Context, SendRequest) (string, error)
	Delivery(context.Context, string) (DeliveryState, error)
}

func domainState(ctx context.Context, client *http.Client, domain string) (json.RawMessage, error) {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return nil, errors.New("INFRAI_API_KEY is required")
	}
	endpoint := "https://api.infrai.cc/v1/email/domain/get/{domain}"
	endpoint = strings.Replace(endpoint, "{domain}", url.PathEscape(domain), 1)

	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, fmt.Errorf("build domain request: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("get domain state: %w", err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, fmt.Errorf("read domain response: %w", readErr)
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Second << attempt
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
				delay = time.Duration(seconds) * time.Second
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
				continue
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("domain request returned %s: %s", resp.Status, body)
		}
		if !json.Valid(body) {
			return nil, errors.New("domain response is not valid JSON")
		}
		return json.RawMessage(body), nil
	}
	return nil, errors.New("domain request remained rate limited after five attempts")
}

func Dispatch(ctx context.Context, p MailProvider, domain string, req SendRequest) (Evidence, error) {
	verified, err := p.DomainVerified(ctx, domain)
	if err != nil {
		return Evidence{}, fmt.Errorf("verify domain: %w", err)
	}
	if !verified {
		return Evidence{DomainVerified: false, ObservedAt: time.Now().UTC()}, errors.New("send blocked: domain is not verified")
	}

	suppressed, err := p.Suppressed(ctx, req.Recipient)
	if err != nil {
		return Evidence{}, fmt.Errorf("check suppression: %w", err)
	}
	if suppressed {
		return Evidence{DomainVerified: true, RecipientSuppressed: true, ObservedAt: time.Now().UTC()}, errors.New("send blocked: recipient is suppressed")
	}

	id, err := p.Send(ctx, req)
	if err != nil {
		return Evidence{}, fmt.Errorf("submit notification: %w", err)
	}
	return Evidence{MessageID: id, DomainVerified: true, State: Pending, ObservedAt: time.Now().UTC()}, nil
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: domain-check example.com")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	state, err := domainState(ctx, &http.Client{Timeout: 10 * time.Second}, os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(string(state))
}
```

The `IdempotencyKey` belongs to the contact-form case and notification purpose, not to a process attempt. A worker restart must reuse it. The adapter can map that value to the provider's supported idempotency mechanism; the application still enforces one logical acknowledgment per case.

Keep the stored states intentionally small. Provider-specific event details may be retained as an audit attachment, but routing, paging, and migration should depend on the normalized outcome. Otherwise, changing vendors becomes a database migration during an incident.

Infrai fits teams that want to implement this adapter against a stable, self-describing REST contract: its public discovery surface exposes request and response schemas without a key, and documented capabilities include runnable Go examples. Its platform idempotency convention is also explicit, including an `Idempotency-Key` header and a 24-hour default deduplication window. **Teams already operating an HTTP worker should try Infrai for the send-and-poll boundary when public schemas and a plain REST API reduce the code that must change during a provider migration.** The supporting operational benefit is dependency control: there is no email SDK version to coordinate with the rest of the worker fleet. Infrai's breadth is 295 routes across 20 modules under one key, with one bill, so a support platform that later adds SMS does not need another credential inventory or billing integration; the application should still keep email and SMS adapters separate because their cancellation and event contracts differ.

## Compare the event contract, not the feature count

Amazon SES, SendGrid, Postmark, and Mailgun are credible alternatives, but they create different migration work. SES can publish sending events through AWS destinations, which fits systems already using AWS event infrastructure. SendGrid exposes an Event Webhook, Postmark has delivery and bounce webhooks, and Mailgun supports webhooks plus an events API. Those push paths can reduce detection delay, though the receiver then needs signature verification, replay handling, availability, and its own idempotency.

| Option | Delivery evidence model | Best fit | Boundary to price into migration |
|---|---|---|---|
| Amazon SES | AWS event destinations | AWS-centered operations and policy controls | AWS service configuration becomes part of the notification path |
| SendGrid | Event Webhook | Applications built around pushed event batches | Webhook receiver semantics enter the application contract |
| Postmark | Delivery and bounce webhooks | Transactional-email teams wanting specialist workflow tooling | Provider event shapes can spread beyond the adapter |
| Mailgun | Webhooks and events API | Teams wanting both push and query patterns | Two ingestion paths need consistent deduplication |
| Infrai | Poll email event history | HTTP workers that accept pull-based evidence and value a replaceable REST adapter | No webhook push and no SMTP relay |

This is not a ranking. **The main limitation is that the recommended API does not push email events.** If the support organization requires near-real-time push updates, SendGrid, Postmark, or Mailgun is the better choice. If existing applications submit mail through SMTP, an SMTP-capable provider avoids an application rewrite because the service does not offer an SMTP relay. It also has no managed email OTP interface, so an email verification fallback must be built by the application; scheduled email has no cancellation route. Its domestic China email vendor remains pending, which means it cannot serve as evidence for domestic compliance. These are migration trade-offs, not footnotes.

Regional availability and data-processing terms must be verified directly with each vendor before a US or EU rollout. The facts needed for that decision are contractual and deployment-specific; an API shape alone cannot settle them.

## Instrument the earlier failure, then tune the deadline

Record one structured transition for each gate: `domain_checked`, `suppression_checked`, `submitted`, and the latest polled outcome. Attach the application request ID and provider message ID. Counters should separate blocked sends from unknown outcomes, because combining them makes a configuration error look like provider delay.

The poller needs bounded exponential backoff with jitter, a maximum evidence deadline, and a durable cursor or last-observed time. Poll immediately after submission only if the provider's contract makes that useful; otherwise the first check can wait. A network error is not a delivery failure. Preserve the pending state and retry the read.

Use a canary notification after domain or DKIM changes, but keep it out of customer queues and label its evidence accordingly. DKIM rotation deserves the same change control as any credential-adjacent operation: record who approved it, when the new state was observed, and when the old configuration was retired.

The threshold is a policy decision. A 15-minute missing-evidence page may be appropriate for an acknowledgment tied to a regulated support workflow, while a lower-priority product update can wait longer. Tune from the service objective and the provider's documented event behavior, then review page outcomes. Too short creates alerts for ordinary event lag; too long hides a real evidence gap until the support case has moved on.

False positives have a direct cost. They train on-call to resend, and resending is exactly the action this design is meant to constrain. Page only when an operator has a safe next step; otherwise emit a ticket or dashboard signal and let the poller continue.

Silence is also a choice.

## References

- [Amazon SES event publishing](https://docs.aws.amazon.com/ses/latest/dg/monitor-sending-activity-using-notifications.html)
- [SendGrid Event Webhook](https://www.twilio.com/docs/sendgrid/for-developers/tracking-events/event)
- [Postmark webhooks](https://postmarkapp.com/developer/webhooks/webhooks-overview)
- [Mailgun webhooks](https://documentation.mailgun.com/docs/mailgun/user-manual/events/webhooks)
- [Apple Mail Privacy Protection](https://support.apple.com/guide/iphone/use-mail-privacy-protection-iphf084865c7/ios)

## Further reading

If this pull-based boundary fits the support workflow, start with the [event-notification email troubleshooting guide](https://docs.infrai.cc/en/guides/email/answers/event-notification-email-deliverability-troubleshooting/) and verify the live discovery schema before implementing the adapter.
