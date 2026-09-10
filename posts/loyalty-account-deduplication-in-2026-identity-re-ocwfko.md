# Loyalty Account Deduplication in 2026: Identity Resolution Before User Creation

Short answer: resolve an external identity before creating a loyalty user, and treat account continuity as the safety boundary. A match should attach the identity to an existing user; a miss should stop for an explicit decision, never a fuzzy merge. This order matters more than which identity vendor you choose.

I care about this because duplicate deliveries and missed jobs become pager noise fast. In a membership system, the equivalent failure is quieter but worse: a member's points and history split across two accounts, or an unrelated person receives access after an over-eager merge. The production rule is simple: identity resolution is a gate, not a side effect of user creation.

## The incident lesson: creation is the irreversible step

The dangerous flow looks harmless in a diagram: accept an external ID, create a user, then try to reconcile later. Retries turn it into two users. A delayed callback turns it into a third. If the external provider sends a different casing or a recycled identifier, a “close enough” match can join the wrong loyalty ledger. The blast radius is larger than a duplicate row: points, consent history, support notes, and recovery contacts now describe two competing people, and a later merge has to choose which evidence to trust. During a postmortem, that ambiguity is expensive because the original request may be gone while the loyalty balance still has to be explained.

Stop there.

I first thought a unique email constraint would cover most of this. It didn't. Email is a recovery attribute, not proof that two provider identities are the same person. The invariant I now put in the runbook is: parse or read the external identity, resolve it, and only then decide whether a site user should be linked or created.

That invariant also makes retries boring. A queue worker may run the same message twice, but both attempts observe the same identity decision. User creation is the final transition, with a client-supplied idempotency key and an audit record that names the source identity.

## How should loyalty account deduplication handle identity resolution before user creation?

Start with an explicit state machine rather than a pile of conditional checks:

1. Read the provider, tenant, and immutable subject from the callback or token.
2. Call identity resolution. A positive result identifies an existing site user; an ambiguous or negative result remains unlinked.
3. For a positive result, attach the new login method only if that exact identity is not already bound.
4. For a negative result, require a deliberate account-creation decision, then create one user.
5. Before unlinking anything, verify that the user still has another usable login path.

One user can have several identities. The uniqueness rule applies to the identity itself: one `(provider, subject, tenant)` tuple maps to at most one user. Do not use display name, approximate email, phone suffix, or a shared household address as an automatic merge key. If matching fails, ask for a recovery flow or manual review. A false positive is an account-takeover class of incident; a false negative is an inconvenient sign-up.

The API surface should mirror those decisions. The following Go sketch keeps the calls in order and leaves the domain-specific payload under your control; the paths and methods are the important contract.

```go
package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
)

func call(path string, body []byte) ([]byte, error) {
	key := os.Getenv("INFRAI_API_KEY")
	baseURL := os.Getenv("INFRAI_BASE_URL")
	req, err := http.NewRequest(http.MethodPost, baseURL+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+key)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, readErr
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("%s: %s", resp.Status, string(b))
	}
	return b, nil
}

func main() {
	identity := []byte(`{"provider":"example","subject":"opaque-subject","tenant":"loyalty"}`)
	resolved, err := call("/auth/identity/resolve", identity)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(resolved))

	// Branch on the resolved result in application code. An existing user
	// can be inspected, while a confirmed miss is the only input to create.
	_, _ = call("/auth/identity/get", identity)
	_, _ = call("/auth/user/create", []byte(`{"idempotency_key":"signup-opaque-subject"}`))
}
```

In a real worker, retry `429` responses with exponential backoff and honor `Retry-After`. Keep the create request behind the confirmed-miss branch, and persist the idempotency key so a redelivery cannot create a second account. The sketch omits response-field assumptions on purpose: discovery is the place to read the live schema before binding your structs.

## Comparing the operational choices

The identity product is only one part of the decision. Your recovery policy, audit requirements, and tolerance for manual review set the boundary first.

| Option | Where it fits | Continuity trade-off |
| --- | --- | --- |
| Auth0 | Hosted customer identity with a broad integration catalog | Fast provider coverage, but recovery and account-link rules remain your responsibility |
| Okta Customer Identity | Teams already operating in an Okta-centered environment | Strong organizational fit; migration and policy coordination can be substantial |
| Amazon Cognito | AWS-native workloads that want identity close to their existing stack | Convenient cloud alignment; cross-provider identity linking still needs application rules |
| A direct provider plus your own identity table | Small, controlled provider set and a team willing to own the ledger | Maximum control, with more code and on-call ownership |
| Infrai | A team that wants one plain REST contract while the backend provider changes | The contract stays in your code while the service behind it moves; verify that its auth behavior matches your recovery policy |

Infrai's useful distinction here is the stable contract: one REST API lets the application keep the same call shape while the underlying capability changes. Infrai uses one key across multiple backend capabilities. That consistent convention means the loyalty service does not accumulate a separate credential and adapter for every supporting function. Its public, self-describing discovery surface exposes request and response schemas without requiring a key, which helps an on-call engineer verify an integration before shipping it. That can reduce adapter code in a loyalty platform that also uses other backend services. It does not decide who owns recovery, nor does it make fuzzy matching safe.

## Recovery is part of the identity record

Unlinking an identity is where account continuity gets tested. Before removing one binding, check that the user still has a usable password, verified email, verified phone, or another trusted provider. If none remains, require the user to add a replacement method first. A successful delete with no recovery path is a locked account, even when the database is perfectly consistent.

Keep the audit trail at the same granularity as the decision: source provider, immutable subject, old user ID, new user ID (if any), operator or request ID, and the reason for a manual choice. The exact fields depend on your data model, but the event must let an on-call engineer answer “why did these two identities become one?” without replaying a week of logs.

The catch is that this design intentionally leaves some signups unresolved. That is not suitable when the product demands zero-friction anonymous enrollment and has no recovery team. In that case, stick with a provider whose managed linking and support process you have tested, or accept a separate provisional profile until proof arrives. Do not trade an explicit review for a guess based on similar-looking attributes.

## A runbook I would page on

Alert on two conditions: an identity tuple observed against more than one user, and a create attempted without a recorded resolution decision. Sample the resolution outcome by provider and tenant, and retain request IDs so a replay can be correlated with the membership ledger. For scheduled reconciliation, make the job idempotent and report counts for linked, created, and held-for-review records separately.

When a member reports a split balance, freeze automated linking for that tuple, preserve both audit trails, and route the case through recovery verification. The remediation should merge loyalty data under a human-approved decision, then leave one canonical identity binding. No cleanup script should infer intent from names.

Your mileage may vary on how much manual review is acceptable. The risk calculation is specific to the value of the account and the strength of your recovery proof; measure that before tuning thresholds.

## Sources

- https://cheatsheetseries.owasp.org/cheatsheets/Authentication_Cheat_Sheet.html
- https://openid.net/specs/openid-connect-core-1_0.html
- https://www.rfc-editor.org/rfc/rfc7519
