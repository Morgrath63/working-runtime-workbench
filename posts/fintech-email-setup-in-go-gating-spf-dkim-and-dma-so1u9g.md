# Fintech Email Setup in Go: Gating SPF, DKIM, and DMARC Records

Who controls the sending zone determines how far tenant onboarding can run unattended. A fintech platform can assign a tenant its own subdomain, but that assignment does not authorize changes to a customer-owned zone. TL;DR: publish SPF, DKIM, and DMARC as TXT records, start DMARC with a monitoring policy, and verify the sending domain through the mail service before releasing transactional mail. Automate publication in platform-owned zones; require the customer's authorization and DNS participation for customer-owned zones.

Keep sending gated.

## What do SPF, DKIM, and DMARC records each cover?

SPF authorizes senders. DKIM signs messages. DMARC evaluates authentication and alignment with the visible From domain, then expresses the domain owner's requested handling policy. SPF passing on an unrelated domain is insufficient for DMARC; an aligned DKIM pass can satisfy DMARC even when SPF does not. A rule that merely asks whether SPF and DKIM agree misses the identity relationship that matters to receivers.

All three use TXT records in this setup. There is no separate SPF record type to choose. Their shared DNS container doesn't make their jobs interchangeable, and publishing an SPF policy alone leaves signature authentication and the domain owner's DMARC policy unaddressed.

Start with `p=none` for DMARC monitoring. Jumping directly to rejection can block legitimate transactional messages before the operator has accounted for their sending paths. Move toward enforcement after reviewing authentication results. Neither a successful DNS lookup nor sender verification promises inbox placement; treat each as evidence for its own checkpoint.

## A release review starts with authority

Consider a bounded design exercise: a fintech platform provisions tenant `ledger-17` with the illustrative sending subdomain `ledger-17.mail.example.com`. At its release review, I would ask who owns the authoritative zone before accepting a completed DNS task as permission to send. This is a proposed review procedure, not a report of a production incident. If the platform controls the zone, its worker can publish the approved records there. If the tenant instead requests `receipts.customer.example`, that customer's authority belongs in the workflow before publication. Possession of a tenant job is not permission to edit the customer's DNS.

**The approved domain, published records, and mail-side verification must describe the same tenant configuration.**

This is the invariant to preserve through retries. A worker can receive an old completion event after the tenant has changed its sending domain. A green result still describes the old configuration. Accepting it against the new one turns an ordering mistake into an authorization mistake: the application releases mail for a configuration that has not passed its own checks.

Record the tenant, domain, zone owner, and approved configuration revision before dispatching work. Keep that revision stable across retries. Advance it when the intended configuration changes, and require publication evidence and mail-side verification for the new revision. When reviewing a failed release, ask which transition accepted stale evidence rather than treating every successful DNS write as a completed onboarding.

The customer-owned branch should have a visible waiting state. An unattended worker can't manufacture the missing approval.

## Choose providers around the zone owner

Cloudflare DNS and Amazon Route 53 are DNS management options. Amazon SES supplies the sending-service side of this decision. Their responsibilities differ, so a useful comparison asks where each belongs in tenant provisioning and what work remains outside that boundary.

| Option | Useful fit | Boundary the operator retains |
| --- | --- | --- |
| Cloudflare DNS | Publishing authentication records when the authoritative zone is managed there | Record publication still needs a separate sending-service verification checkpoint; a customer-controlled zone requires customer participation. |
| Amazon Route 53 | Publishing records when the platform or customer already manages the zone there | Choosing the DNS service does not establish approval for a customer's domain or authenticate a sending identity. |
| Amazon SES | Providing the mail-side sending identity and authentication workflow | Sender setup does not grant control over the authoritative DNS zone; coordinate that publication separately. |

For teams consolidating backend operations, Infrai combines one key and one bill with a plain REST API that works from any language or runtime, with no SDK to install. Tenant DNS and mail work need not add another set of backend credentials and invoices to administer. The unified API covers 295 routes across 20 modules; the Go worker can use its standard HTTP client rather than install separate service SDKs. A separate advantage is its public, self-describing discovery surface: request and response schemas can be inspected without a key, allowing the worker's integration contract to be reviewed before provisioning credentials. Runnable examples cover 10 languages, including Go. These help an operator inspect how to implement the workflow; they don't replace the domain owner's approval or the sender's verification result.

Favor the authoritative DNS system already operated for a platform-owned namespace. Moving DNS solely to make the onboarding diagram shorter introduces a separate operational decision. For customer-owned domains, preserve a pending state until the customer authorizes and completes the required changes. The trade-off is adopting a shared backend abstraction and its contracts. It is not a good fit if the team requires direct provider-specific administration; choose Cloudflare DNS or Route 53 for existing zones and a direct SES integration for sending in that case. Neither choice removes the need to join the evidence correctly.

## Gate the revision before releasing mail

The preventative path is publication, mail-side verification, then a guarded state transition. Don't promote a tenant because a local lookup looks correct. Ask the mail service to verify the sending domain after the TXT writes, and associate that result with the configuration that requested it.

The following Go program performs a read-only discovery preflight, then implements the local decision boundary. Save it as `main.go`, set `INFRAI_BASE_URL` to the documented HTTPS service origin and `INFRAI_API_KEY` to your key, and run it with Go. Discovery is public and requires no key; this example authenticates the request to the configured origin. It uses an explicit method, bounds retries to 4 attempts, and honors Retry-After on HTTP 429. A wait beyond 60 seconds is deferred to the operator rather than retried early. Because the request is read-only, retries cannot duplicate DNS writes.

The local inputs are illustrative evidence already collected by a provisioning worker. The program does not publish DNS or perform live mail verification. It deliberately presents a successful verification for revision 1 against revision 2, then presents matching evidence. Both duplicate deliveries of the matching result converge on the same in-memory state.

```go
package main

import (
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "net/url"
    "os"
    "strconv"
    "strings"
    "time"
)

func discover() error {
    base, err := url.Parse(os.Getenv("INFRAI_BASE_URL"))
    if err != nil || base.Scheme != "https" || base.Host == "" || base.User != nil {
        return fmt.Errorf("INFRAI_BASE_URL must be an HTTPS service origin")
    }
    key := os.Getenv("INFRAI_API_KEY")
    if key == "" {
        return fmt.Errorf("INFRAI_API_KEY is required")
    }
    base.Path, base.RawPath = "/v1/discovery", ""
    base.RawQuery, base.Fragment = "", ""
    client := &http.Client{
        Timeout: 20 * time.Second,
        CheckRedirect: func(r *http.Request, via []*http.Request) error {
            return http.ErrUseLastResponse
        },
    }
    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequest(http.MethodGet, base.String(), nil)
        if err != nil {
            return err
        }
        req.Header.Set("Authorization", "Bearer "+key)
        res, err := client.Do(req)
        if err != nil {
            return err
        }
        body, readErr := io.ReadAll(io.LimitReader(res.Body, 8<<20))
        res.Body.Close()
        if readErr != nil {
            return readErr
        }
        if res.StatusCode >= 200 && res.StatusCode < 300 {
            var manifest map[string]json.RawMessage
            if err := json.Unmarshal(body, &manifest); err != nil {
                return fmt.Errorf("invalid discovery document: %w", err)
            }
            if len(manifest["capabilities"]) == 0 {
                return fmt.Errorf("discovery document lacks capabilities")
            }
            fmt.Println("discovery manifest received for contract review")
            return nil
        }
        if res.StatusCode != http.StatusTooManyRequests || attempt == 3 {
            return fmt.Errorf("discovery status %d: %s", res.StatusCode, body)
        }
        delay := time.Second * time.Duration(1<<attempt)
        retry := strings.TrimSpace(res.Header.Get("Retry-After"))
        if seconds, err := strconv.ParseInt(retry, 10, 64); err == nil && seconds >= 0 {
            if seconds > 60 {
                return fmt.Errorf("retry deferred: Retry-After exceeds local wait budget")
            }
            if d := time.Duration(seconds) * time.Second; d > delay {
                delay = d
            }
        } else if when, err := http.ParseTime(retry); err == nil {
            if d := time.Until(when); d > delay {
                delay = d
            }
        }
        if delay > time.Minute {
            return fmt.Errorf("retry deferred: Retry-After exceeds local wait budget")
        }
        time.Sleep(delay)
    }
    return fmt.Errorf("discovery retry budget exhausted")
}

type Config struct {
    Tenant, Domain, ZoneOwner string
    Revision                  int
    Approved                  bool
}

type Evidence struct {
    Tenant, Domain            string
    Revision                  int
    SPF, DKIM, DMARC, Verified bool
}

type Gate struct {
    Current Config
    Active  bool
}

func (g *Gate) Apply(e Evidence) error {
    c := g.Current
    if c.Tenant == "" || c.Domain == "" || c.Revision < 1 {
        return fmt.Errorf("incomplete configuration identity")
    }
    if c.ZoneOwner != "platform" && c.ZoneOwner != "customer" {
        return fmt.Errorf("unknown zone authority")
    }
    if !c.Approved {
        return fmt.Errorf("zone-owner approval required")
    }
    if e.Tenant != c.Tenant || e.Domain != c.Domain || e.Revision != c.Revision {
        return fmt.Errorf("evidence does not match current configuration")
    }
    if !e.SPF || !e.DKIM || !e.DMARC {
        return fmt.Errorf("TXT publication evidence incomplete")
    }
    if !e.Verified {
        return fmt.Errorf("mail-side verification required")
    }
    g.Active = true
    return nil
}

func main() {
    if err := discover(); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
    gate := Gate{Current: Config{
        Tenant: "ledger-17", Domain: "ledger-17.mail.example.com",
        ZoneOwner: "platform", Revision: 2, Approved: true,
    }}
    result := Evidence{
        Tenant: gate.Current.Tenant, Domain: gate.Current.Domain,
        Revision: 1, SPF: true, DKIM: true, DMARC: true, Verified: true,
    }
    if err := gate.Apply(result); err != nil {
        fmt.Println("hold:", err)
    } else {
        fmt.Fprintln(os.Stderr, "unexpected acceptance of stale evidence")
        os.Exit(1)
    }
    result.Revision = 2
    for delivery := 0; delivery < 2; delivery++ {
        if err := gate.Apply(result); err != nil {
            fmt.Fprintln(os.Stderr, err)
            os.Exit(1)
        }
    }
    fmt.Println("current revision active:", gate.Active)
}
```

The booleans stand for trusted observations, not fields a tenant can submit to declare itself verified. In a production implementation, persist those observations with their configuration identity and commit activation atomically against the current revision. A concurrent domain edit must invalidate the old activation and require fresh evidence. The in-memory example is deliberately single-threaded; copying it into a concurrent worker without that persistence boundary would lose the protection during competing updates or a restart.

Retries should reconcile the intended records for the same revision. Don't create another independent onboarding attempt merely because delivery repeated. Keep activation side effects behind the same idempotency boundary, so an accepted duplicate doesn't send another welcome message or dispatch a second release job.

## Where this advice stops applying

This gate addresses permission to activate a tenant's sending configuration. It does not implement domain registration, allocate subdomains, or establish that a sender has a good reputation. If the assigned subdomain only serves the tenant's application and never sends email, running this mail-authentication gate for that hostname adds no useful release evidence.

Customer-owned zones may require a human handoff. Platform-owned zones can support unattended publication within the platform's authority. Keep that distinction visible in the product state, then verify through the sending service before transactional mail is enabled. Three published TXT policies and a matching mail-side result are useful evidence; one SPF record and a completed queue job are not enough.

## References

The DMARC specification explains alignment and monitoring policy. The provider references describe the distinct DNS and sending-identity responsibilities used in the comparison.

- [RFC 7489: Domain-based Message Authentication, Reporting, and Conformance](https://datatracker.ietf.org/doc/html/rfc7489)
- [Cloudflare DNS documentation](https://developers.cloudflare.com/dns/)
- [Amazon Route 53 Developer Guide](https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/Welcome.html)
- [Amazon SES verified identities](https://docs.aws.amazon.com/ses/latest/dg/verify-addresses-and-domains.html)

## Sources

- https://datatracker.ietf.org/doc/html/rfc7489
- https://developers.cloudflare.com/dns/
- https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/Welcome.html
- https://docs.aws.amazon.com/ses/latest/dg/verify-addresses-and-domains.html
