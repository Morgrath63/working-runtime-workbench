# Go Service Discovery Caching for Mail (DNS Ownership and Registry Freshness)

Customer-owned DNS gives a school control of its sending identity, but it also puts record publication outside the platform's deployment transaction. TL;DR: use DNS records for stable internal names when their caching behavior fits the required cutover window; use a service registry when workers need explicit endpoint membership and timely removal. Keep SPF, DKIM, and DMARC publication separate from either discovery mechanism. A successful worker deployment does not establish that a school's domain authenticates correctly.

Track 2 readiness checks: where a worker can connect, and which domain that worker is authorized to send for.

Neither implies the other.

## What actually failed in the deployment model?

Consider a hypothetical edtech release: a scheduler queues assignment reminders, mail workers consume them, and one school uses a customer-owned sending domain while another uses a platform-owned domain. A worker release changes the internal relay destination. At the same time, the first school is publishing authentication records. This is an incident exercise, not a report of a measured outage: the failure to investigate is a release gate that treats successful internal resolution as proof that both schools are ready to send.

That gate answers the wrong question.

An internal DNS lookup can establish an address for the relay name. It does not establish the SPF authorization of the envelope sender, validate a DKIM signature, or establish alignment with the visible From domain. DMARC evaluates aligned SPF or DKIM authentication, as specified in RFC 7489. Merely finding three TXT records is not an end-to-end authentication test. The message's identifiers and its actual authentication results matter.

The invariant is operational: each readiness observation needs an identity, a purpose, and a bounded lifetime. An endpoint observation belongs to a particular discovery scope. A mail-authentication observation belongs to a sending configuration. Persisting one generic `ready` flag erases that distinction and makes a later retry capable of acting on an earlier configuration.

For an incident timeline, record the queued message identity, tenant, sending-configuration revision, discovery observation, and submission outcome. Those are proposed diagnostic fields, not a claim that a DNS server or registry supplies them. They let an operator ask whether the wrong endpoint was selected, the wrong sending identity was used, or an ambiguous submission was retried. Those are different recovery paths.

## Should internal service discovery use DNS records or a registry for frequent deploys?

Deploy frequency is a useful prompt, but it is a poor decision rule on its own. Ask how long a worker may keep using an old endpoint, whether that endpoint remains available during draining, and what happens to work already submitted through an open connection. Ten releases behind a stable relay name can require less discovery churn than one release that immediately removes every old address. Those are illustrative release patterns, not benchmark results.

DNS record TTLs bound caching in the DNS model described by RFC 1034 and RFC 1035. They do not close application connections or drain an old process. A client may also have application-level caching whose behavior needs separate inspection. Negative responses have caching rules too, described by RFC 2308; creating a previously missing name is not a reason to assume every consumer will immediately retry it.

| Discovery choice | Appropriate operating condition | Required release discipline |
| --- | --- | --- |
| Stable internal DNS name | An old destination can remain usable through the tolerated stale window | Observe client resolution and connection behavior; drain before removal |
| DNS SRV records | Consumers implement SRV lookup and its port, priority, and weight semantics | Verify the client actually uses SRV; an address lookup alone is insufficient |
| Service registry | Consumers need explicit membership changes and can implement the registry's freshness contract | Define reconnect, missed-update recovery, health ownership, and stale-state behavior |

A registry is a protocol and an operating commitment. Its removal event helps only if the worker receives it and changes its endpoint selection. A disconnected consumer still needs a rule for its last observed state. Conversely, DNS can be sufficient when a stable service address hides worker replacement and the release process accommodates cached answers. The trade-off is client complexity versus control over freshness. A registry is not suitable as a shortcut around an unexamined connection pool: the client still has to stop selecting removed endpoints and finish or abandon work already in flight. A DNS-only design has a different limitation: it cannot meet an immediate endpoint-removal requirement through cached address records alone. The table is a decision aid, not a claim that all registries share one consistency model. Write the stale-state policy down before implementation, including who owns the decision to stop submissions when discovery becomes unavailable.

Keep old endpoints drainable.

Test the failure path before choosing. Interrupt discovery updates, hold an existing connection open, and remove an endpoint only after the planned drain interval. Measure the last use of that endpoint by real consumers. If the result exceeds the tolerated window, change the client or the rollout contract; switching the discovery label alone cannot fix it.

## DNS ownership changes the mail release gate

For a customer-owned zone, the platform can prepare the expected records and observe their publication, but completion depends on whoever can change that zone. For a platform-owned zone, the platform can coordinate publication directly with its sending configuration. This changes release coordination and responsibility. It does not change the authentication standards.

Take two illustrative identities: `school.example` and `courses.platform.example`. The names are placeholders, not live infrastructure. Keep the expected envelope-sender domain, DKIM signing domain and selector, visible From domain, and DMARC policy associated with the relevant sending configuration. A worker moving to a new relay should not silently replace those tenant-specific identifiers with platform defaults merely because the platform zone is easier to update.

SPF authorizes sending hosts for the identity being checked. DKIM signs messages using a signing domain and publishes verification keys through DNS. DMARC adds policy and identifier alignment with the visible From domain. RFC 7489 permits a DMARC pass through either aligned SPF or aligned DKIM; it does not require both mechanisms to pass. Publishing all three is the configuration task here, while evaluating an actual message is the acceptance task.

Start the runbook with evidence from a controlled message using the intended sending path. Inspect the identifiers and authentication results together. An observation from one school's domain does not validate another school's domain, and a successful authentication check does not promise inbox placement. Acceptance by a receiver and placement in an inbox are separate outcomes.

Ownership also determines who gets the next action. If a customer's publication is pending, hold that sending configuration's activation and give the zone owner the precise expected record. If the platform owns the zone, keep the publication step in the platform's release workflow.

Don't redeploy around missing records.

## Put a freshness guard before the submission boundary

The preventative code path should make stale observations explicit. The following Go example is a local preflight predicate for queued work. Its deadlines are application policy, not invented DNS TTLs, registry leases, or guarantees about receiver caches. Populate them from the separately defined discovery and mail-verification policies.

```go
package mailgate

import (
	"errors"
	"time"
)

type Snapshot struct {
	Tenant             string
	SendingRevision    string
	Endpoint           string
	DiscoveryChecked   bool
	DiscoveryValidUntil time.Time
	MailVerified       bool
	MailValidUntil     time.Time
}

func Check(now time.Time, tenant, revision string, s Snapshot) error {
	if tenant == "" || revision == "" {
		return errors.New("missing sending identity")
	}
	if s.Tenant != tenant || s.SendingRevision != revision {
		return errors.New("sending configuration changed; revalidate")
	}
	if !s.DiscoveryChecked || s.Endpoint == "" ||
		!now.Before(s.DiscoveryValidUntil) {
		return errors.New("refresh endpoint discovery before submission")
	}
	if !s.MailVerified || !now.Before(s.MailValidUntil) {
		return errors.New("revalidate mail identity before submission")
	}
	return nil
}
```

Equality with a deadline is expired. A zero deadline is expired for a current timestamp.

Bind `MailVerified` to the exact configuration revision that was tested; it must not mean only that a TXT query returned some text. The example returns a reason to hold work, and it deliberately leaves scheduling, persistence, and protocol-specific verification to the caller.

Check again near submission. If discovery changes while an item waits in the queue, refresh the endpoint observation without creating a new logical reminder. If the sending revision changes, revalidate that configuration before proceeding. A change after this local check is still possible, so the downstream connection and submission path must handle its own errors. This predicate is a guard, not a transaction spanning DNS and a mail receiver.

Preserve a durable logical reminder key across retries, for example the tuple of tenant, assignment, recipient, and reminder occurrence. Claim that key through a persistence operation with an appropriate uniqueness constraint before concurrent workers can act on it. A new endpoint or a new attempt number must not create a new reminder identity.

Do not promise exactly-once delivery. A crash after the relay accepts a message but before the worker records success leaves an uncertain outcome. A local uniqueness constraint prevents competing local claims; it cannot atomically commit an external mail acceptance. Record uncertainty and reconcile it under an explicit policy before blindly resending a class-wide reminder.

## When this separation needs less machinery

A single stable internal relay and a single platform-controlled sending identity may need only a modest release gate. An application with no customer-owned sending domains can omit the customer publication handoff. Keep the checks proportional to the number of independently changing identities and destinations.

The boundaries still matter. A registry cannot publish a customer's DMARC policy, and a DNS change cannot prove that a queued reminder was submitted only once. Before the next release, define the acceptable stale interval, assign zone ownership, and document how uncertain submissions are handled. Those decisions turn a missed reminder investigation into a bounded recovery procedure.

## Sources

References:

- RFC 1034, Domain Names: Concepts and Facilities — https://datatracker.ietf.org/doc/html/rfc1034
- RFC 1035, Domain Names: Implementation and Specification — https://datatracker.ietf.org/doc/html/rfc1035
- RFC 2308, Negative Caching of DNS Queries — https://datatracker.ietf.org/doc/html/rfc2308
- RFC 2782, DNS SRV Resource Records — https://datatracker.ietf.org/doc/html/rfc2782
- RFC 7208, Sender Policy Framework — https://datatracker.ietf.org/doc/html/rfc7208
- RFC 6376, DomainKeys Identified Mail Signatures — https://datatracker.ietf.org/doc/html/rfc6376
- RFC 7489, Domain-based Message Authentication, Reporting, and Conformance — https://datatracker.ietf.org/doc/html/rfc7489
- Go standard library, time package — https://pkg.go.dev/time
