# Go Change Authorization — Vanity Asset CNAME and Signed URL Generation

Authorize each tenant's vanity asset hostname before publishing its CNAME or enabling signed URL generation, and preserve the marketplace company's MX records on their own label. A customer-owned zone needs a customer-approved change; a platform-owned zone needs an accountable platform operator. Treat those as different deployment permissions.

TL;DR: A vanity asset CNAME selects a DNS destination; it doesn't grant access to a tenant's files. Authenticate the tenant before issuing a signed URL, bind the signature to the approved hostname and resource, and make the serving layer verify that binding. For a Node.js application, keep DNS reconciliation outside the request path and use the same signing contract across the application and asset gateway. Preserve the company's MX records throughout.

The operational entry point is change authorization. A marketplace may be moving company mail to a provider while onboarding merchant asset hostnames, and the same zone can contain both changes. Combining them in one broad replacement creates an avoidable failure domain. The runbook below treats mail routing and asset access as separate changes with separate evidence of success.

## Who owns the zone when the change runs?

Record the authoritative zone, its operator, the requested label, and the tenant authorized to use that label before scheduling a change. Customer-owned means the customer controls publication: your job should produce the requested records and observe the result. Platform-owned means your controller can publish records within its delegated authority. Neither arrangement makes a request from a tenant sufficient proof that the tenant controls an arbitrary hostname.

For illustration, use `market.example` for company mail and `images.shop.example` for one merchant's assets. The company mail provider supplies the required MX destinations and preferences. Keep that requested MX set attached to the company domain in the change record. The merchant separately approves a CNAME from `images.shop.example` to `assets.market.example`. These names are examples, not deployable service endpoints.

Keep labels separate.

RFC 1034 describes CNAME alias behavior, and RFC 2181 clarifies that an alias cannot also hold ordinary data at the same name. An asset CNAME at a distinct label therefore avoids asking the company mail label to serve incompatible roles. RFC 2181 also requires an MX target to resolve to address records rather than be an alias. Validate the provider's requested destinations instead of improvising a convenient asset alias as a mail target. The practical trap is treating a zone as a replaceable blob: an otherwise correct asset change can delete unrelated mail records if the deployment owns more of the zone than its task requires.

The ownership trade-off is concrete. Customer publication adds a coordination boundary and can delay activation. Platform publication gives the operator direct execution authority and therefore requires tighter permission scope and an audit trail.

Authority determines scheduling.

Choose based on who is entitled to change the zone, then schedule around that constraint.

## Gate the job on observed state

Use a durable change record with the tenant identifier, approved hostname, desired record, previous record set, and an operation identifier. Treat retries as reconciliation: read the current state, compare it with the intended state, and apply only the authorized difference. Repeated delivery of the same job must not create another hostname binding or erase unrelated records. A job being acknowledged is evidence of processing, not evidence that DNS answers have converged. Separate publication from activation. First establish authorization for the hostname. Then publish or request the DNS change, check the authoritative answer, and check the resolution path used by the serving system. Only activate signed-link issuance after the hostname is bound to the correct tenant and the HTTPS endpoint can serve it with a valid certificate. DNS resolution alone doesn't establish an HTTPS identity.

An expired worker lease deserves special attention. A replacement worker may be reconciling the same change while the original worker returns from an external operation. Use a generation check before committing activation so an older attempt cannot activate a superseded hostname. This is a proposed controller contract; its implementation must match the storage system's concurrency guarantees.

Stop on ambiguity.

Don't infer success from a timeout or blindly roll back a zone because one resolver still returns a cached answer. RFC 1034 describes TTL-based caching; publication and cache expiry are different events. Preserve the previous record set and the observed answers so the next operator can distinguish a failed write from a successful write whose earlier answer remains cached.

## How should signed URL generation bind a tenant's vanity asset hostname?

A signing service needs an authenticated tenant context, an active hostname binding, and authorization for the requested object. It should obtain the hostname from that binding rather than accept a caller-supplied host as authoritative. If a gateway uses a shared signing key, include the tenant identifier in the authenticated payload and verify that the requested resource belongs to that tenant. A signature authenticates the claims you put into it; it cannot repair a missing authorization check.

The following Go example defines a small signing contract that a Node.js caller can implement with identical bytes. It's deliberately a signing core, not a complete HTTP handler. Its inputs must already have passed authentication, hostname selection, and object authorization. The example uses HMAC-SHA-256, Unix expiry seconds, and a fixed payload version. JSON field order is part of this particular contract: another implementation must serialize the same ordered fields without whitespace. Use shared fixtures before deploying implementations in two languages.

```go
package assetlink

import (
    "crypto/hmac"
    "crypto/sha256"
    "encoding/base64"
    "encoding/json"
    "errors"
    "net/url"
    "strconv"
    "time"
)

type claims struct {
    Version int    `json:"v"`
    Tenant  string `json:"tenant"`
    Host    string `json:"host"`
    Object  string `json:"object"`
    Expires int64  `json:"expires"`
}

// host is a validated lowercase DNS name from an active tenant binding.
// tenant and object are authorized identifiers using letters, digits, - or _.
func Sign(key []byte, tenant, host, object string, now time.Time) (string, error) {
    if len(key) < 32 || tenant == "" || host == "" || object == "" {
        return "", errors.New("invalid signing inputs")
    }
    expiry := now.Add(5 * time.Minute).Unix()
    payload, err := json.Marshal(claims{1, tenant, host, object, expiry})
    if err != nil {
        return "", err
    }
    mac := hmac.New(sha256.New, key)
    _, _ = mac.Write(payload)
    query := url.Values{}
    query.Set("v", "1")
    query.Set("tenant", tenant)
    query.Set("expires", strconv.FormatInt(expiry, 10))
    query.Set("sig", base64.RawURLEncoding.EncodeToString(mac.Sum(nil)))
    u := url.URL{
        Scheme: "https",
        Host: host,
        Path: "/objects/" + object,
        RawQuery: query.Encode(),
    }
    return u.String(), nil
}
```

The 300-second lifetime and minimum 32-byte key length are example policy choices, not DNS requirements. Provision keys from a cryptographically secure source. Shorter link lifetimes reduce the useful lifetime of a copied bearer link but require more frequent issuance; longer lifetimes make revocation and hostname retirement more consequential.

Expiry isn't DNS TTL.

Verification must reconstruct the exact claims from the canonical request host, authorized tenant binding, object identifier, and strictly parsed expiry. Reject duplicate query parameters, unknown versions, invalid identifier characters, expired links, and signatures that fail a constant-time comparison such as Go's `hmac.Equal`. Define how a trusted proxy supplies the original hostname before using forwarded headers. Check the tenant-to-object relationship again at the serving boundary.

Cache behavior belongs in this contract too. Require authorization before serving a protected cached response, or use an explicitly private caching policy. A cache key that ignores a security-relevant host or tenant can defeat an otherwise correct signature check. Never log the complete signed URL: the query string is a bearer credential during its validity window.

## Verify mail delivery and asset access independently

Before activation, exercise the implementation with two synthetic tenants and one authorized object per tenant. A valid link should work on its approved hostname. Changing the tenant, object, hostname, expiry, or signature must fail. Include an expired link and a duplicate-parameter request. These are acceptance cases for the proposed contract, not reported test results.

Test the job separately: deliver the same operation twice, interrupt it after publication but before activation, and supersede it while an older worker is still running. The desired outcome is one current binding with an intelligible history. Measure queue age and the time spent awaiting customer publication separately; a pending customer change shouldn't look like a stuck worker. Keep signing failures, hostname activation failures, and DNS observation failures distinguishable in telemetry.

For mail, compare the authoritative MX answer with the approved provider record set, then perform a controlled delivery test. Inspect authentication results as a separate check. RFC 7489 specifies DMARC evaluation in terms of identifier alignment with authenticated SPF or DKIM results; changing inbound MX records doesn't by itself establish that alignment. Keep mail routing evidence and sender-authentication evidence in separate acceptance fields. A received test message proves one delivery path worked at that moment, not that every cached answer has expired.

The release gate should identify the responsible operator and the next observation to make. Avoid a single green status that hides whether publication, TLS readiness, mail delivery, or asset authorization actually passed.

## Roll back the binding before reopening issuance

Pause new signed-link issuance for the affected binding when validation fails. Reconcile the DNS label to its recorded previous state only after checking that a newer authorized change hasn't replaced it. Restoring DNS won't instantly replace answers already held in caches, and moving an alias won't revoke a bearer link that a serving endpoint still accepts. Enforce retirement at the authorization boundary as well.

Keep company MX rollback independent of asset rollback. Restore only records owned by the failed change, repeat the relevant delivery or access checks, and retain the observed answers with the operation identifier. For customer-owned zones, give the customer the precise restoration request and wait for evidence of publication before recording recovery. For platform-owned zones, apply the same verification after the controller restores the records.

Close the change when both the intended state and its access behavior are verified. The durable outcome is a tenant binding with clear authority, a signing contract that survives retries and host changes, and company mail whose routing can be explained without guessing which deployment last touched the zone.

## References

- RFC 1034, Domain Names: Concepts and Facilities: https://datatracker.ietf.org/doc/html/rfc1034
- RFC 2181, Clarifications to the DNS Specification: https://datatracker.ietf.org/doc/html/rfc2181
- RFC 7489, Domain-based Message Authentication, Reporting, and Conformance: https://datatracker.ietf.org/doc/html/rfc7489
- RFC 2104, HMAC: Keyed-Hashing for Message Authentication: https://datatracker.ietf.org/doc/html/rfc2104
- RFC 9111, HTTP Caching: https://datatracker.ietf.org/doc/html/rfc9111
- Go crypto/hmac package: https://pkg.go.dev/crypto/hmac
- Go encoding/json package: https://pkg.go.dev/encoding/json
- Go net/url package: https://pkg.go.dev/net/url
