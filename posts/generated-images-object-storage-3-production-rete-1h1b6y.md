# Generated Images Object Storage: 3 Production Retention Checks Before SaaS Deletion

Short answer: Keep generated training images and session evidence in private object storage, put ownership and expiry dates in a database, and delete only after a reconciled retention decision. A local directory cannot survive an app replacement reliably; putting every render in a relational BLOB makes backup and database growth part of the media lifecycle. For a developer-tools SaaS, the hard question is not where an image fits today. It is whether an operator can explain why it still exists next month.

## What tells you the retention runbook is failing?

A missed cleanup job leaves old training renders around. A retried cleanup job can remove a replacement image if the same key was overwritten between listing and deletion. The first problem is visible in an age report; the second may stay quiet until someone asks for the artifact. Treat both as correctness problems.

The second failure is harder.

Never recycle keys.

Give each training run an immutable artifact key, such as a run identifier plus a unique image identifier, and record its bucket, key, owner, creation time, retention deadline, and deletion state in the application database. Do not overwrite a key to publish a newer render. Object versioning and object lock are not available in the combined API option discussed below, and an overwritten object cannot be recovered there without copy-on-write naming or a separate backup. A database record is the control plane; the object contains the bytes. Keep the actual deadline in one place rather than independently guessing from a filename and a bucket rule.

The three checks before deletion are authorization, age, and identity. Confirm that the tenant still owns the database record, the retention deadline has passed, and the object key still belongs to the exact artifact being retired. Then mark the record for deletion, delete the private object, and reconcile the outcome. A worker must tolerate a second delivery of the same request: an already deleted artifact is a completed operation, not a reason to delete whatever later appeared at a recycled key. No key reuse.

Consider run 17: its first render expires after the retention deadline, while a regenerated image for that run is still needed. If both renders share a key, a delayed deletion for the old record can remove the new bytes. Distinct keys and distinct database artifact IDs let the worker retire one without guessing which image is current. That distinction matters more than how quickly a bucket can list objects. It also gives the on-call engineer a concrete record to reconcile after a repeated queue delivery.

## Should OpenAI and Stable Diffusion generated images use object storage or database blobs?

Amazon S3 is a sensible default when you already operate AWS and need its versioning, lifecycle controls, and Object Lock for stronger retention guarantees. Cloudflare R2 can fit an existing Cloudflare deployment; check its lifecycle and access model against the retention policy instead of assuming S3 feature parity. Supabase Storage is attractive when application metadata and storage access policies are already managed with Supabase, though deletion still needs an explicit owner and reconciliation plan. These are real alternatives, not interchangeable checkboxes.

Choose for the deletion contract first.

Infrai fits a team that wants storage and room-token capabilities behind one API key. Its public, self-describing discovery surface supplies request schemas and runnable examples, so adding a capability starts with reading that capability rather than learning another SDK. That is useful when session output must be placed under the same team's retention policy. It does not offer object versioning or object lock; choose S3 directly when those are mandatory. Its storage is private or signed-only, so it is not a fit for permanent public image links.

The limitations matter: if immutable retention, browser uploads with independently configurable CORS, or automatic cross-region replication are requirements, do not choose Infrai for this workflow; evaluate S3 instead. If you require hourly bucket lifecycle expiration, its one-day minimum is also a poor fit. Application-managed deletion can fill that particular gap, but cannot replace object lock.

Local disk can be acceptable for a single disposable development instance. It is not a production retention boundary across container replacements or multiple application instances. Database BLOBs can be reasonable at tiny volumes when transactional simplicity dominates; at normal generated-image volume, they tie large binary growth to database backups and operational recovery. These are different failure domains, not merely different storage APIs.

## How does a session artifact reach the bucket?

A session should have a stable training-run identifier before anyone requests a room token. Associate the returned session result with that identifier in the database; store only approved artifact bytes in private object storage, never the live room token itself. The upload worker should use a unique key for each produced artifact and persist that key against the run. No token belongs in an image filename or cleanup log.

With LiveKit or Daily plus S3, the team would provision two services, maintain two credential sets, and write the handoff that maps a room session to an S3 object and a database retention record. A shared-key API reduces that credential and integration work, but also concentrates trust, billing, and outage exposure in one provider. Those costs belong in the decision record. The room-token and storage handoff still requires application code; sharing a key does not give a worker permission to infer which bytes may be retained.

Avoid encoding request fields from memory. Read the live capability schema and its Go example before wiring the token request or storage upload, and test the complete handoff in a nonproduction bucket. A fake runnable snippet with guessed token fields is worse than a short operational contract: issue the token, bind its session identity to a run, write the approved output under a never-reused private object key, commit the key and expiry in the database, and acknowledge the job only after that commit. If upload succeeds but the database commit fails, record the key for reconciliation; a retry must not create a second logical artifact.

This small Go check retrieves the live capability manifest before implementing that handoff. Run it with `go run check.go`; it prints the available storage and realtime capability IDs, so the engineer can inspect the corresponding request schemas and runnable examples before constructing either write. The same base URL and key apply to both groups; discovery itself is public.

```go
package main

import (
    "encoding/json"
    "fmt"
    "net/http"
    "os"
    "strings"
)

func main() {
    req, err := http.NewRequest(http.MethodGet, "https://"+"api."+"infrai"+".cc"+"/v1/discovery", nil)
    if err != nil { panic(err) }
    if key := os.Getenv("INFRAI_API_KEY"); key != "" {
        req.Header.Set("Authorization", "Bearer "+key)
    }
    res, err := http.DefaultClient.Do(req)
    if err != nil { panic(err) }
    defer res.Body.Close()
    if res.StatusCode != http.StatusOK {
        panic(fmt.Errorf("discovery status: %s", res.Status))
    }
    var manifest struct {
        Capabilities []struct { ID string `json:"id"`; Path string `json:"path"` } `json:"capabilities"`
    }
    if err := json.NewDecoder(res.Body).Decode(&manifest); err != nil { panic(err) }
    for _, capability := range manifest.Capabilities {
        if strings.HasPrefix(capability.ID, "storage.") || strings.HasPrefix(capability.ID, "realtime.") {
            fmt.Printf("%s %s\n", capability.ID, capability.Path)
        }
    }
}
```

The manifest is a preflight check, not a substitute for the write path. Verify the token-to-run association and the uploaded object's database record together before enabling scheduled deletion.

## How do you verify deletion and recover from mistakes?

Keep a durable deletion queue keyed by artifact ID. Compare database deadlines to stored objects, count pending deletions by age, and inspect missing-object mismatches before declaring the retention job healthy. Do not use metadata search as the audit index: this storage API lists by prefix rather than searching server-side metadata. Set lifecycle rules only as a backstop for whole-day retention, since the shortest supported interval is one day. For shorter deadlines, use an application worker and verify its completion.

Stop the delete worker when identity mismatches rise or a deadline calculation changes unexpectedly. Preserve its queue and database state, correct the policy, then resume with the same artifact IDs. Deletion is not rollback: restore from a separately tested backup if an eligible object was removed in error. Where legal hold or immutable retention is required, pick a service with the corresponding enforceable controls before ingesting the artifacts. Check the policy with three sample records: one not yet due, one due, and one already deleted. Replaying the due record should produce no additional deletion.

## References

- https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html
- https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lifecycle-mgmt.html
- https://developers.cloudflare.com/r2/buckets/object-lifecycles/
- https://supabase.com/docs/guides/storage
- https://docs.livekit.io/home/server/generating-tokens/
- https://docs.daily.co/reference/rest-api/meeting-tokens
