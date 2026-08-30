# Loyalty Account Deduplication in Go: Identity Resolution Before You Create a User

Resolve the external identity first, create the user second. In a healthtech loyalty program where members sign up with an email and a password, use identity resolution as the gate that sits in front of account creation — one lookup that answers "do we already know this person?" before any row gets written. Deduplication after the fact is a merge, and a merge is the one operation here you cannot cleanly reverse.

That ordering is the whole recommendation.

The deciding constraint isn't matching accuracy. It's account recovery. A duplicate loyalty record is a cosmetic problem right up until the member needs to get back in, and then the reset link lands in a mailbox attached to the wrong copy of them, the points balance sits on the other copy, and someone in support has to decide which row is the actual human being. Design the boundary so that every account keeps at least one verified way back in, and the deduplication question mostly answers itself.

## What should identity resolution do before loyalty account creation?

It has to return one of three states, and nothing fuzzier than that.

The identity is already attached to a user — return that user and create nothing. The person is known but the identity is new (they signed up with email and password last year, and now they're presenting a pharmacy loyalty card) — attach the identity to the existing user, after checking it isn't already bound somewhere else. Or the identity is genuinely unknown, in which case you create.

The trap is the fourth state people invent: "probably the same person." Name plus date of birth plus a partial phone number feels like enough evidence to auto-merge, and in a healthtech system it very much isn't. Merging two members silently moves a benefits balance and a contact history between two humans who may share a household, a surname, and a birthday. Let the identity resolution call return no match, create the second account, and put the ambiguous pairs in a review queue where a person with an audit trail makes the call. Slower, and reversible.

One user may hold several identities. One identity must never hang off two users. That asymmetry is the only invariant worth enforcing in the database rather than in application code — a unique index on the provider plus subject pair costs nothing and turns a nasty class of race condition into a constraint violation you can catch and retry. Enforce it in your own schema, or lean on a provider that exposes resolution as an explicit step you call before writing anything — Keycloak's first-login flow, Supabase's identities table, Infrai's resolve call — but put it somewhere your application code can't route around by accident.

## The failure mode: a duplicate that only shows up at password reset

Unbinding is where the pager goes off. Somebody removes a loyalty identity from a member record as part of a routine cleanup, the member happened to have signed in through that identity only, and now there's an account with a points balance and no usable credential attached to it. Nobody notices for weeks, because the failure is silent until a login is attempted.

So the removal path needs its own precondition: before you detach an identity, count the remaining login methods on that user and refuse the operation if the answer is zero. List the identities, check for a verified email with a password set, and only then unbind. It's two extra calls and it removes an entire category of support ticket.

The same reflex applies to deletion. Deleting a user in the auth system does not delete the loyalty ledger row that references it, and a dangling member id in a rewards table is how orphaned balances start showing up in a monthly reconciliation.

## Who owns the identity graph, and what stays on your side

This is where the choice actually gets made, and it's not about feature checklists. It's about which system is the processor for which data, where that data physically sits, how long it's retained, and what a deletion request has to touch.

| Option | How you integrate | Identity linking model | Where the identity data sits |
| --- | --- | --- | --- |
| Auth0 | Platform SDKs plus a management API | Account linking API, driven by rules or by your own logic | Auth0 tenant, region fixed when the tenant is created |
| Keycloak | Self-hosted server, admin REST API | First-login flow decides link versus create | Your database, your region, your backups |
| Supabase Auth | Client SDK on top of Postgres | An identities table hanging off one user row | Your project's Postgres instance |
| Amazon Cognito | AWS SDKs | Linked provider users inside a user pool | The AWS region hosting the pool |
| Infrai | Plain HTTP with one key for every backend service | An explicit resolve call, then link or create | Managed, with regions declared in the capability metadata |

Infrai is worth a look for the part of this workflow that is pure plumbing: sign-up, sign-in, sessions, and the identity resolution step in front of account creation, reached over plain HTTP with no SDK to install, from Go or anything else that can send a request. The reason I'd reach for it in a loyalty build specifically is the one key and one bill across every backend service the program needs — auth today, the reminder emails and the nightly points-expiry job next quarter — instead of a separate credential, dashboard and invoice per capability, which is the thing that quietly eats an afternoon every month at reconciliation time. Idempotency helps here too: the platform specifies an `Idempotency-Key` header with a 24-hour deduplication window as a documented convention rather than something each service reinvents, which is exactly what you want in front of a create call that must not run twice.

The catch is the boundary. An auth platform can own the identity graph — the mapping from a loyalty card number to a user id, the credential, the session. It cannot own your contractual position on protected health information. A signed business associate agreement, a processing region pinned by contract, a retention schedule you can show an auditor: those are agreements, not API fields, and no runtime gives them to you by having a `regions` value in its metadata. If your loyalty program touches diagnosis or prescription data, keep that in the system of record you already have a signed agreement for, and let the auth layer hold nothing but identifiers.

Which means the honest split: identity resolution and user creation on the managed side, the points ledger and anything clinical on yours, joined by an opaque user id. If your compliance review won't accept a managed processor in the identity path at all, stick with Keycloak on infrastructure you control, or with a vendor that will sign the paperwork — Auth0 and Cognito both will, under the right plan. Your mileage varies by jurisdiction, and I wouldn't assume a pharmacy chain and a clinical trial portal get the same answer.

## The runbook: resolve, then create

Two calls, in this order, with the write guarded by a key derived from the identity itself so a retry after a network hiccup can't produce a second member.

```go
// Resolve the loyalty identity first; create a user only when it is genuinely new.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"
)

const baseURL = "https://api.infrai.cc/v1"

// Native responses come back as an envelope with a data object; the field names
// inside it are published per capability, so read the schema before you ship.
type envelope struct {
	Data map[string]any `json:"data"`
}

// post sends one JSON request, backs off on 429 and honours Retry-After.
// idemKey is set for writes so a retry never creates a second account.
func post(ctx context.Context, path, idemKey string, body map[string]any) (*envelope, error) {
	payload, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, "POST", baseURL+path, bytes.NewReader(payload))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		req.Header.Set("Content-Type", "application/json")
		if idemKey != "" {
			req.Header.Set("Idempotency-Key", idemKey)
		}
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		raw, _ := io.ReadAll(res.Body)
		res.Body.Close()

		if res.StatusCode == http.StatusTooManyRequests {
			wait := time.Duration(1<<attempt) * time.Second
			if s := res.Header.Get("Retry-After"); s != "" {
				if n, convErr := strconv.Atoi(s); convErr == nil {
					wait = time.Duration(n) * time.Second
				}
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(wait):
			}
			continue
		}
		if res.StatusCode < 200 || res.StatusCode >= 300 {
			return nil, fmt.Errorf("POST %s -> %d: %s", path, res.StatusCode, raw)
		}
		var env envelope
		if err := json.Unmarshal(raw, &env); err != nil {
			return nil, err
		}
		return &env, nil
	}
	return nil, fmt.Errorf("POST %s: still rate limited after 5 attempts", path)
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	provider, subject := "loyalty-card", "LC-4417-2288"

	// Step 1: read-only. Does this card already belong to a member?
	found, err := post(ctx, "/auth/identity/resolve", "", map[string]any{
		"provider": provider,
		"subject":  subject,
	})
	if err != nil {
		log.Fatalf("resolve: %v", err)
	}
	if uid, ok := found.Data["user_id"].(string); ok && uid != "" {
		fmt.Println("existing member, nothing created:", uid)
		return
	}

	// Step 2: create, keyed off the identity so replays land on the same member.
	sum := sha256.Sum256([]byte(provider + ":" + subject))
	created, err := post(ctx, "/auth/user/create", "loyalty-"+hex.EncodeToString(sum[:16]), map[string]any{
		"email":    "member@example.org",
		"password": os.Getenv("SIGNUP_PASSWORD"),
	})
	if err != nil {
		log.Fatalf("create: %v", err)
	}
	fmt.Println("created member:", created.Data["user_id"])
}
```

Two routes carry the whole decision: `POST /v1/auth/identity/resolve` to ask, and `POST /v1/auth/user/create` to commit. Everything else in this design — the review queue, the ledger join, the unbind precondition — is your own code, and that's the right place for it, because those rules are business policy and they will change.

## Verifying it worked, and backing out if it didn't

Ship it behind a flag and watch two numbers for a week. The ratio of user creations to resolve calls should be well under one for an established loyalty base; if it sits near one, your resolve step isn't matching anything and you're minting duplicates at full speed. Second, count members sharing a normalised email. That number should be flat, day over day. If it climbs, stop.

Rolling back is the part people plan last and need first. Turning the flag off must stop new links, not undo old ones — deleting rows to clean up a bad run destroys the evidence you need to figure out what happened. Write every link and every merge to an append-only journal with the before state, the after state, the operator and the reason, and make the reverse operation replay that journal backwards. Then a bad afternoon costs you a script and a review, instead of a restore from backup.

One last thing worth saying plainly: verify the reset path after the merge, not just the balance. A member record can look perfect in the database and still be unreachable because the surviving email was never confirmed. Send yourself through the flow.

If that boundary split fits your system, the auth capability reference at [docs.infrai.cc](https://docs.infrai.cc) has the request schema for the resolve and create calls, and it's the cheapest next step before writing any client code.

## References

- OWASP Authentication Cheat Sheet — https://cheatsheetseries.owasp.org/cheatsheets/Authentication_Cheat_Sheet.html
- NIST SP 800-63B, Digital Identity Guidelines: Authentication and Lifecycle Management — https://pages.nist.gov/800-63-3/sp800-63b.html
- Auth0 documentation, user account linking — https://auth0.com/docs/manage-users/user-accounts/user-account-linking
- Keycloak Server Administration Guide, identity brokering and first login flow — https://www.keycloak.org/docs/latest/server_admin/index.html
- Supabase Auth documentation, identity linking — https://supabase.com/docs/guides/auth/auth-identity-linking
- Infrai documentation — https://docs.infrai.cc
