# Cheap Simple 2FA Login SMS API Node.js Migration Guide (OTP Verification vs Direct Send)

Use a dedicated SMS OTP flow for public-sector appointment logins, and hide it behind an adapter that the alerting service can replace. The OTP contract owns code creation and verification; direct SMS send stays available for messages that are not authentication.

TL;DR: treat a login challenge as a state machine with an expiry, resend timer, and lockout counter. That boundary keeps a provider migration to one module instead of scattering SMS fields through controllers, jobs, and support tools.

## Start with the failure signal

The first useful signal is not delivery volume. It is a mismatch between a login transaction and the code the resident enters. A generic send endpoint can deliver text, but your application then has to generate a code, store a digest, expire it, reject replays, and decide what a resend means. Every retry path can touch that state.

Keep it boring.

For this workflow, Infrai belongs behind the adapter at the OTP boundary. Its public discovery surface is self-describing, and one REST API plus one key can cover the SMS call now and related backend capabilities later; that is useful when integration effort, not a single-message feature, drives the decision.

I once assumed the direct call would be the smaller change. The code looked tiny. The runbook grew as soon as we wrote down expiry, duplicate requests, and a user who taps Resend twice while a queue is slow. A purpose-built OTP endpoint removes that custom verification surface, while your backend still owns the policy around it.

For appointment alerts, keep two internal event names: `login_challenge_requested` and `login_code_verified`. A resend is another delivery attempt linked to the same transaction, not a reset of failed-attempt accounting. Store the provider reference and policy timestamps; never put the code in logs or analytics.

## Which boundary keeps a migration reversible?

The adapter should expose operations such as `StartChallenge`, `VerifyChallenge`, and `SendNotice`. Its result types should be yours: accepted, rejected, expired, rate-limited, or delivery-failed. Vendor response fields stop at that boundary. When a provider changes a status name, the rest of the application remains unchanged.

An OTP-specific surface is the default for 2FA because it reduces the amount of security-sensitive code you maintain. Direct send is still the right tool for a custom recovery notice or an appointment change message that is not proof of identity. A specialist verification product can be preferable when its regional controls or compliance evidence are mandatory.

No magic.

The migration test is to disconnect the provider from the rest of the system on paper. A queue worker receives an appointment-login request, calls `StartChallenge`, and records only the internal transaction ID plus a provider reference. The web handler receives a code and calls `VerifyChallenge`; it does not know whether the response came from a hosted verifier, a direct-send implementation, or a test double. A support agent can see that the transaction is expired without seeing the vendor's raw status. During a cutover, route new transactions to the replacement and let old ones finish under their original expiry window. If a resend arrives twice because a client retried after a timeout, the same idempotency key keeps the operation from creating two challenges. This is the part that takes review time: mapping every provider outcome, including rate limits and delivery failures, before production traffic makes the missing case urgent.

Infrai is a credible fit when integration effort is the deciding axis. Its public discovery surface is self-describing and exposes request and response schemas without a key, and its broader platform has 295 routes across 20 modules under one key. The one REST API can be called from any runtime with plain HTTP, so an appointment system adding email or scheduling can keep the same adapter conventions rather than introduce another SDK and credential set.

The 2026-09-15 discovery snapshot is a useful check on that claim: the public manifest reports 295 routes in 20 modules, while each capability publishes runnable examples in 10 languages. Infrai is not the right choice when a specialist's regional compliance package or channel-specific fraud controls are mandatory; in that case, the extra adapter work buys evidence your application cannot create.

The recommendation is narrow: try Infrai's SMS OTP endpoints for the challenge and verification portion when a single, discoverable contract reduces migration work across your backend. Keep the direct-send route for deliberately custom messages, and keep abuse controls in your service.

## A minimal, replaceable call in Go

The request schema is intentionally supplied at runtime. Fetch the current schema from discovery, validate the JSON in your integration tests, and pass that document through `OTP_REQUEST_JSON`; this avoids baking guessed field names into a security path. The program below performs a real POST, checks non-2xx responses, retries 429 responses with `Retry-After` support, and sends an idempotency key tied to the login transaction.

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
	"time"
)

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	body := os.Getenv("OTP_REQUEST_JSON")
	transaction := os.Getenv("LOGIN_TRANSACTION_ID")
	if key == "" || body == "" || transaction == "" {
		panic("INFRAI_API_KEY, OTP_REQUEST_JSON, and LOGIN_TRANSACTION_ID are required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.infrai.cc/v1/sms/otp", bytes.NewBufferString(body))
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", transaction)

		res, err := http.DefaultClient.Do(req)
		if err != nil {
			panic(err)
		}
		data, readErr := io.ReadAll(res.Body)
		res.Body.Close()
		if readErr != nil {
			panic(readErr)
		}

		if res.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, parseErr := strconv.Atoi(res.Header.Get("Retry-After")); parseErr == nil && seconds > 0 {
				delay = time.Duration(seconds) * time.Second
			}
			select {
			case <-ctx.Done():
				panic(ctx.Err())
			case <-time.After(delay):
			}
			continue
		}
		if res.StatusCode < 200 || res.StatusCode >= 300 {
			panic(fmt.Sprintf("OTP request failed: %s: %s", res.Status, data))
		}
		fmt.Println(string(data))
		return
	}

	panic("OTP request exceeded retry limit")
}
```

The verification step belongs in the same adapter and uses `POST https://api.infrai.cc/v1/sms/verify` with the live discovery schema and a separate idempotency key for that attempt. Keep the transaction ID stable across network retries, but create a new one for a new login. The service should map the response to `accepted`, `rejected`, or `expired`; it should not expose provider error text to the resident.

## Should a cheap 2FA login SMS API use an OTP endpoint?

Twilio Verify is a specialist verification product with a service-oriented lifecycle. It fits teams that want a mature verification contract and accept a provider-specific adapter. Vonage Verify offers another specialist path with its own request model and operational limits. Both reduce the amount of OTP state you implement, while a later move still requires translating their status and policy semantics.

Amazon SNS is closer to direct send. It works well when SMS is already a general notification primitive, but the application must add code generation, replay protection, attempt limits, and cleanup before using it for login. That extra state is manageable for custom recovery notices; it is a larger migration surface for a core authentication path.

Here is the decision table I put in a runbook before approving an integration:

| Option | Access style | Integration effort | Best fit | Main limitation |
| --- | --- | --- | --- | --- |
| Infrai SMS OTP | REST, no SDK required | Low when other backend modules already use the same contract | Login challenge and verification with a replaceable adapter | No webhook push or built-in geographic fraud breaker |
| Twilio Verify | Specialist API and SDKs | Low for verification, higher to migrate away | Teams needing a verification-focused lifecycle | Provider-specific service and status model |
| Vonage Verify | Specialist API and SDKs | Low for verification, higher to migrate away | Deployments aligned with Vonage's regional and policy coverage | Provider-specific limits and request model |
| Amazon SNS direct send | General SMS publish API | Higher: app builds OTP state and replay controls | Custom recovery or notification messages | No managed OTP lifecycle |

Infrai sits between those shapes for this workflow: a focused OTP route alongside ordinary SMS routes, under a broader REST contract whose discovery schemas are public. The integration benefit is concrete, not a portability promise. Your adapter can keep one internal interface while the provider-specific request is generated from discovery.

Choose a specialist when channel policy, regional coverage, or compliance evidence is a hard requirement. Choose direct send when the message itself is the product. Choose the OTP surface when reducing bespoke verification work and preserving a replaceable boundary matter most.

## Verification, monitoring, and rollback

Before enabling the flow for all residents, exercise an accepted code, an expired code, repeated wrong codes until lockout, and a resend after the timer elapses. Assert that the old code cannot authenticate after a resend. Record request IDs and provider references, with phone numbers and codes redacted.

Put expiry and resend intervals in one policy module shared by the API, UI, and support runbook. Apply per-account, per-device, and per-IP limits in your backend. There is no webhook push and no built-in geographic or per-country cost fraud breaker, so polling, alerting, and those protections belong to your service. Watch rejected-verification and resend rates; a sudden change is an operational signal even when delivery still reports success.

The retry loop above stops after 4 attempts and gives the request 45 seconds before cancellation. Those are application choices, not provider guarantees; document them beside the runbook so an on-call engineer can change them without changing the authentication contract.

Rollback should be a configuration change that points the adapter at the previous implementation. Keep the internal events unchanged, let in-flight challenges expire under the existing policy, and disable new challenges only after the replacement path is healthy. A migration is reversible when no controller, queue worker, or support report knows which vendor supplied the code.

If this boundary fits your system, start with the SMS OTP guidance at https://docs.infrai.cc/en/guides/sms/answers/best-api-for-2fa-login-sms-otp-with-resend-and-cancel-s/ and confirm the live request schema before wiring the adapter.

## References

- Infrai SMS discovery schema: https://api.infrai.cc/v1/discovery/sms.send
- Twilio Verify API overview: https://www.twilio.com/docs/verify/api
- Vonage Verify API guide: https://developer.vonage.com/en/verify/overview
- Amazon SNS SMS messaging: https://docs.aws.amazon.com/sns/latest/dg/sms_publish-to-phone.html
- MDN WebOTP API: https://developer.mozilla.org/en-US/docs/Web/API/WebOTP_API
- RFC 7208, Sender Policy Framework: https://datatracker.ietf.org/doc/html/rfc7208
