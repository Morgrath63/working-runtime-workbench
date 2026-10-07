# Encrypt PDF Reports — Deliver Passwords Out of Band with Signed Audit Trails

The operational constraint is separation: anyone who can read the PDF delivery channel must not automatically receive its password. **TL;DR:** render the monthly marketplace report, hash the exact bytes, encrypt those bytes, record a signed manifest, and send a one-time password reference through a separately authenticated channel. Never place the PDF and password in the same message, log, queue payload, or support ticket.

I have been paged for missed cron jobs and duplicate queue deliveries. In a monthly reporting pipeline, either failure can turn a sound encryption design into an audit problem: a seller receives no statement, or receives two messages whose attachments and passwords may not belong to the same generation. The invariant is stricter than "encrypt before sending." One report generation must have one stable identity, one byte digest, one manifest, and independently recorded outcomes for document delivery and secret delivery.

One identity. Two channels.

## How should Node.js encrypt a PDF and deliver its password out of band?

Out-of-band delivery reduces the consequence of one channel being exposed. It does not repair a weak password, a compromised recipient account, malicious endpoint software, or a server that logs secrets. PDF encryption is access control around a file; it is not proof that a human read the right report. ISO 32000-2 defines the PDF format, while the cryptographic and operational choices still belong to the system around it.

For a marketplace statement, bind the report to immutable business coordinates: seller ID, reporting month, currency, and generation number. A random job ID alone is poor evidence because an investigator cannot tell which business obligation it represented. Do not put the password in that identity.

The runtime does not change that rule. An Express handler should validate and authorize the request, allocate the generation ID, persist the work item, and return without carrying either artifact through the request lifecycle. A worker can use a conforming PDF implementation behind the `EncryptPDF` boundary shown below. Keeping the HTTP layer thin prevents a client retry or proxy timeout from becoming a second report generation, while the durable job preserves enough context to reconcile the month later. The same boundary also keeps PDF policy out of route code: renderer upgrades, signing-key rotation, and encryption-policy changes become versioned inputs to the manifest rather than invisible deployment details.

The signature and encryption have different jobs. The encrypted PDF limits access to document contents. A digital signature over a manifest establishes who asserted the report identity and digest, and reveals later modification of that manifest. Calculate the digest from the final PDF bytes before encryption if the audit question is "which rendered report did we approve?" Record a second digest of the encrypted artifact if the question also includes "which attachment did we transmit?" Those are two legitimate objects. Conflating them causes painful incident review.

## Build one evidence chain, not two unrelated sends

Treat rendering, approval, encryption, and delivery as state transitions under a single generation ID. The queue message should carry references and identifiers, not document bytes or passwords. Durable storage holds the artifacts; a secrets system holds a high-entropy, single-use password value with a short retrieval policy; the database holds the manifest and delivery receipts.

A useful manifest contains the report coordinates, generation ID, renderer version, plaintext digest, encrypted-artifact digest, encryption policy identifier, signing-key identifier, creation time, and destination identifiers for both channels. The signature covers a canonical serialization of those fields. Canonicalization matters: signing ordinary map output can produce different byte sequences for the same logical values. RFC 8785 specifies a JSON canonicalization scheme when JSON is the manifest format.

The two deliveries should be retryable independently. The document sender uses an idempotency key derived from the generation ID and the literal channel name. The password-notification sender uses another key. A worker that crashes after a successful send but before saving its receipt can retry; the downstream adapter must return the original result for the same key instead of sending again. Exactly-once claims are not required. Stable effects are.

| Record | Contains | Must exclude |
|---|---|---|
| Render manifest | Business coordinates, renderer version, plaintext digest | Password and raw report data |
| Artifact receipt | Encrypted digest, destination, delivery receipt, timestamp | Password |
| Secret receipt | Secret reference, authenticated destination, timestamp | Secret value |
| Audit event | Generation ID, transition, actor or workload identity | PDF bytes and credentials |

Keep those records append-oriented. A mutable row may be convenient for current status, but it should be a projection of immutable events, not the only evidence left after a retry.

Receipts beat guesses.

## Make the preventative path deterministic

The following Go sketch shows the control path around cryptographic primitives. `EncryptPDF` and `SignCanonical` are interfaces on purpose: their implementations must be selected against the PDF encryption requirements and the organization's key-management policy. The example does not pretend that wrapping arbitrary bytes with a generic cipher produces a conforming encrypted PDF.

```go
package statements

import (
    "context"
    "crypto/sha256"
    "encoding/hex"
    "errors"
    "fmt"
    "time"
)

type Job struct {
    GenerationID string
    SellerID     string
    Month        string
    Currency     string
}

type Manifest struct {
    GenerationID    string `json:"generation_id"`
    SellerID        string `json:"seller_id"`
    Month           string `json:"month"`
    Currency        string `json:"currency"`
    PlainSHA256     string `json:"plain_sha256"`
    EncryptedSHA256 string `json:"encrypted_sha256"`
    PolicyID        string `json:"policy_id"`
    SigningKeyID    string `json:"signing_key_id"`
    CreatedAt       string `json:"created_at"`
}

type Dependencies interface {
    LoadRenderedPDF(context.Context, Job) ([]byte, error)
    CreateSecret(context.Context, string) (string, []byte, error)
    EncryptPDF(context.Context, []byte, []byte, string) ([]byte, error)
    StoreArtifact(context.Context, string, []byte) (string, error)
    SignCanonical(context.Context, Manifest, string) ([]byte, error)
    AppendApproved(context.Context, Manifest, []byte, string, string) error
    SendDocument(context.Context, string, string, string) (string, error)
    SendSecretNotice(context.Context, string, string, string) (string, error)
    AppendReceipt(context.Context, string, string, string) error
}

func Publish(ctx context.Context, d Dependencies, j Job) error {
    if j.GenerationID == "" || j.SellerID == "" || j.Month == "" {
        return errors.New("incomplete report identity")
    }
    plain, err := d.LoadRenderedPDF(ctx, j)
    if err != nil {
        return fmt.Errorf("load rendered report: %w", err)
    }
    secretRef, password, err := d.CreateSecret(ctx, j.GenerationID)
    if err != nil {
        return fmt.Errorf("create secret: %w", err)
    }
    encrypted, err := d.EncryptPDF(ctx, plain, password, "marketplace-statement-v1")
    clear(password)
    if err != nil {
        return fmt.Errorf("encrypt report: %w", err)
    }
    artifactRef, err := d.StoreArtifact(ctx, j.GenerationID, encrypted)
    if err != nil {
        return fmt.Errorf("store encrypted report: %w", err)
    }
    manifest := Manifest{
        GenerationID: j.GenerationID, SellerID: j.SellerID,
        Month: j.Month, Currency: j.Currency,
        PlainSHA256: digest(plain), EncryptedSHA256: digest(encrypted),
        PolicyID: "marketplace-statement-v1", SigningKeyID: "reports-signing-v1",
        CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
    }
    signature, err := d.SignCanonical(ctx, manifest, manifest.SigningKeyID)
    if err != nil {
        return fmt.Errorf("sign manifest: %w", err)
    }
    if err := d.AppendApproved(ctx, manifest, signature, artifactRef, secretRef); err != nil {
        return fmt.Errorf("record approval before delivery: %w", err)
    }
    docReceipt, err := d.SendDocument(ctx, artifactRef, j.SellerID, j.GenerationID+":document")
    if err != nil {
        return fmt.Errorf("send document: %w", err)
    }
    if err := d.AppendReceipt(ctx, j.GenerationID, "document", docReceipt); err != nil {
        return fmt.Errorf("record document receipt: %w", err)
    }
    secretReceipt, err := d.SendSecretNotice(ctx, secretRef, j.SellerID, j.GenerationID+":secret-notice")
    if err != nil {
        return fmt.Errorf("send secret notice: %w", err)
    }
    return d.AppendReceipt(ctx, j.GenerationID, "secret-notice", secretReceipt)
}

func digest(b []byte) string {
    sum := sha256.Sum256(b)
    return hex.EncodeToString(sum[:])
}
```

There is a deliberate ordering choice here: approval evidence is durable before either external send. This can leave an approved generation with no delivery receipt, which a reconciler can safely detect and retry. Sending first creates the worse gap: a recipient may possess an artifact that the audit system cannot identify.

Do not log `password`, the returned secret bytes, signed access links, or full destination addresses. Structured logs need the generation ID, stage, attempt count, duration, outcome class, and stable error code. Metrics should count reports stuck in each state and age the oldest unfinished generation. Alert on age against the monthly reporting objective, rather than paging on every transient send failure.

## Test the failures that erase trust

A happy-path test proves very little. Inject a crash after each external effect and before its receipt is saved. Run the same queue message concurrently twice. Verify that the adapter observes one idempotency key, that the manifest signature still validates, and that the stored encrypted digest matches the delivered artifact receipt.

Test mismatches aggressively: wrong seller, wrong month, regenerated PDF under an existing generation ID, rotated signing key, expired secret reference, and document success followed by secret-channel failure. The last case is not rollbackable. Mark it as a partial delivery and retry only the secret notification; replacing both objects silently would destroy the connection to the original receipt.

Do not regenerate on retry.

I first reach for retry logic when a queue job misses its deadline. Duplicate-delivery incidents change that instinct: retry policy without idempotent effects merely repeats uncertainty. The reconciliation query is more valuable than another retry loop. It should answer, for every expected seller and month, which generation is approved, which digest was delivered, and which channel receipt is absent.

Cryptographic tests also need negative cases. Change one byte in the canonical manifest and require signature verification to fail. Change one byte in the encrypted artifact and require the stored digest comparison to fail. Attempt access with the wrong password. Confirm that backups and diagnostic exports do not contain secret values.

## Where this design does not fit

Out-of-band passwords are a poor fit when recipients already have a strongly authenticated portal that can authorize each download and record access without exporting a reusable file secret. They also add little when both channels terminate in the same compromised mailbox or device. In those cases, channel separation is theater.

The design may still be required by a contractual workflow, but document that limitation honestly. Prefer short retention, explicit revocation behavior, and recipient re-verification over pretending a permanent PDF password can be recalled. Digital signatures do not provide confidentiality, and encryption does not establish business approval; retain both controls only when each answers a real audit question.

The decision rule is compact: use encrypted PDF plus out-of-band secret delivery when offline possession is required and the two channels have meaningfully independent authentication. Bind every transition to one generation ID and signed digests. If independent channels or durable receipts are unavailable, stop calling the workflow auditable.

## Sources

- https://www.iso.org/standard/75839.html
- https://www.rfc-editor.org/rfc/rfc8785
- https://csrc.nist.gov/pubs/fips/180-4/upd1/final
- https://csrc.nist.gov/pubs/fips/186-5/final
- https://owasp.org/www-project-logging-cheat-sheet/
