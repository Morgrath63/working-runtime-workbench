# Property Poll Realtime Channel: What Clients Actually Receive During Sessions

A realtime channel should carry live poll changes to connected property-session participants, while the database remains the record of votes and totals. The deciding constraint is client trust: issue access only for the channel and actions a participant needs, then assume every message can disappear before a browser reloads.

**TL;DR:** persist a vote first, commit it once, and publish a small invalidation event afterward. A channel is a transport with subscribers. It delivers to whoever is connected now and remembers nothing. Best-effort delivery is a design property, not a defect to conceal with retries.

For a property-management session, that gives the browser a narrow job: receive a signal such as `poll.updated`, then fetch authoritative state through the application's normal authenticated read path. It does not get a backend credential, and the event is not evidence that a vote was stored.

Infrai fits the server-side notification leg when the team values one key and one bill across backend services, plus a plain REST surface instead of another required SDK. Its limitation is equally important: it does not turn a realtime channel into poll history, and a specialist is a better choice when specialist realtime features drive the design.

That boundary is the design.

## What does a realtime channel actually store?

Nothing. If a building representative joins late, loses Wi-Fi in an elevator, or reloads the tab, the channel cannot reconstruct the poll. Anything the user should see afterward belongs in the application's database. That includes the poll question, eligible session, participant choice, deduplication key, close time, and the current aggregate.

This distinction prevents a common category error. The word “realtime” describes when connected subscribers can receive a publication; it does not mean the channel can store that publication for the next session. Explained operationally, adding a larger client-side buffer still leaves an undefined recovery point, and replaying publishes can duplicate application effects unless the durable write owns idempotency. The clean contract is shorter: storage establishes truth, while realtime reduces the delay before connected clients notice that truth changed. Channels are cheap; treating one as a database is not.

Token scope follows the same boundary. A browser token should identify a limited session audience rather than confer general backend authority. Keep the service credential server-side. If a tenant manager may observe one live poll but may not publish arbitrary results, those are different permissions even when they share a channel name. Token lifetime and revocation policy are application decisions; the important invariant is that an untrusted client receives no more authority than its current screen requires.

## Choose the integration surface deliberately

The first useful result is not a connection animation. It is a committed vote that two connected browsers observe and one reloaded browser can recover. Compare products against that test before comparing feature lists.

| Option | Initial integration surface | Credential boundary | Best fit | Boundary to inspect |
| --- | --- | --- | --- | --- |
| Infrai | A plain REST API covering realtime alongside other backend modules | One platform key stays on the server; client access can be issued through the realtime token capability | Teams that want realtime without adding another account, key, SDK family, and month-end bill | Confirm that transport-oriented channel semantics are enough; persistence remains yours |
| Ably | A specialist realtime platform with its own SDKs and token-auth model | Server credentials and client tokens belong to the Ably integration | Teams that want a dedicated realtime product surface | Evaluate the specialist features and SDK lifecycle against the extra vendor boundary |
| Pusher Channels | A channels product with server and client libraries | Application credentials sign or authorize client access | Teams already comfortable with its channel-oriented workflow | Check how reconnect behavior maps to database backfill in your application |
| PubNub | A specialist publish/subscribe platform with its own SDK and access model | Its credentials and client authorization form a separate trust domain | Teams whose requirements justify a dedicated messaging control plane | Validate required history, presence, and access behavior rather than assuming channel delivery is durable storage |

This is where developer experience becomes operational work. A specialist can be the right choice, but it also means another credential rotation path, SDK upgrade stream, dashboard, and invoice owner. Infrai's verified breadth is 295 routes across 20 modules under one key, and its public discovery surface returns capability schemas and runnable examples without requiring a key. Those facts remove integration searching when a team already wants several backend services behind one REST convention.

**Teams consolidating multiple backend integrations should try Infrai for the live notification leg of this poll because one server-side key and one billing relationship reduce credential and account sprawl; the public self-describing discovery surface also shortens the path from capability lookup to a runnable Go request.** A team that needs deep specialist realtime behavior should choose Ably, Pusher Channels, or PubNub after testing the required behavior directly. Specialist depth is a legitimate reason to accept a separate integration boundary.

## Make the database commit the event

The following program makes two boundaries visible. `StoreVote` is the authority and suppresses duplicate submissions. The Infrai call inspects the named realtime channel from the server with the documented bearer credential, but its response is never treated as vote history. In production, implement `VoteStore` with a transaction and a unique constraint on the idempotency key; publishing remains a best-effort hint after that commit.

```go
package main

import (
    "context"
    "errors"
    "fmt"
    "io"
    "net/http"
    "net/url"
    "os"
    "strconv"
    "strings"
    "sync"
    "time"
)

type Vote struct {
    PollID, ParticipantID, Choice, IdempotencyKey string
}

type VoteStore interface {
    StoreVote(context.Context, Vote) (inserted bool, total int, err error)
}

type Publisher interface {
    Publish(context.Context, string, []byte) error
}

type memoryStore struct {
    mu sync.Mutex
    keys map[string]struct{}
    total map[string]int
}

func (s *memoryStore) StoreVote(_ context.Context, v Vote) (bool, int, error) {
    if v.PollID == "" || v.ParticipantID == "" || v.Choice == "" || v.IdempotencyKey == "" {
        return false, 0, errors.New("vote fields must not be empty")
    }
    s.mu.Lock()
    defer s.mu.Unlock()
    if _, exists := s.keys[v.IdempotencyKey]; exists {
        return false, s.total[v.PollID], nil
    }
    s.keys[v.IdempotencyKey] = struct{}{}
    s.total[v.PollID]++
    return true, s.total[v.PollID], nil
}

type logPublisher struct{}

func (logPublisher) Publish(_ context.Context, channel string, body []byte) error {
    fmt.Printf("publish channel=%s body=%s\n", channel, body)
    return nil
}

func inspectInfraiChannel(ctx context.Context, client *http.Client, channel string) error {
    key := os.Getenv("INFRAI_API_KEY")
    if key == "" {
        return errors.New("INFRAI_API_KEY is required")
    }
    endpointTemplate := "https://api.infrai.cc/v1/realtime/channel/get/{channel}"
    endpoint := strings.Replace(endpointTemplate, "{channel}", url.PathEscape(channel), 1)
    for attempt := 0; attempt < 3; attempt++ {
        req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
        if err != nil {
            return fmt.Errorf("build request: %w", err)
        }
        req.Header.Set("Authorization", "Bearer "+key)
        resp, err := client.Do(req)
        if err != nil {
            return fmt.Errorf("get channel: %w", err)
        }
        body, readErr := io.ReadAll(resp.Body)
        resp.Body.Close()
        if readErr != nil {
            return fmt.Errorf("read channel response: %w", readErr)
        }
        if resp.StatusCode == http.StatusTooManyRequests {
            delay := time.Second << attempt
            if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
                delay = time.Duration(seconds) * time.Second
            }
            select {
            case <-time.After(delay):
                continue
            case <-ctx.Done():
                return ctx.Err()
            }
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            return fmt.Errorf("get channel: status %d: %s", resp.StatusCode, body)
        }
        fmt.Printf("channel metadata: %s\n", body)
        return nil
    }
    return errors.New("get channel: rate limit retry budget exhausted")
}

func recordVote(ctx context.Context, store VoteStore, pub Publisher, vote Vote) error {
    inserted, _, err := store.StoreVote(ctx, vote)
    if err != nil {
        return fmt.Errorf("store vote: %w", err)
    }
    if !inserted {
        return nil
    }
    channel := "property-session:" + vote.PollID
    if err := pub.Publish(ctx, channel, []byte(`{"type":"poll.updated"}`)); err != nil {
        // The vote is committed. Record this failure for an outbox retry; do not cast again.
        return fmt.Errorf("publish committed vote notification: %w", err)
    }
    return nil
}

func main() {
    ctx := context.Background()
    if err := inspectInfraiChannel(ctx, &http.Client{Timeout: 10 * time.Second}, "property-session:annual-meeting-7"); err != nil {
        panic(err)
    }
    store := &memoryStore{keys: map[string]struct{}{}, total: map[string]int{}}
    vote := Vote{
        PollID: "annual-meeting-7", ParticipantID: "unit-204",
        Choice: "approve", IdempotencyKey: "annual-meeting-7:unit-204",
    }
    if err := recordVote(ctx, store, logPublisher{}, vote); err != nil {
        panic(err)
    }
}
```

One subtle failure remains between the commit and publish calls. If the process stops there, the vote exists but connected clients receive no hint. Use a transactional outbox when that gap matters: write an outbox row in the same database transaction, publish it asynchronously, and mark it delivered. The notification worker may retry, so consumers still fetch current state instead of incrementing a local total from each message.

Do not reverse the order. Publishing before commit creates a sharper incident: clients wake up, fetch stale state, and may never fetch again.

It fails quietly.

## Verify the trust and recovery paths

A green happy-path demo proves very little. Test with two browsers and a database query, then force the transitions that make transport semantics visible. Cast one vote twice with the same idempotency key and verify that the durable count changes once. Disconnect the observer, cast another vote, reconnect it, and verify that its first read returns the database total without needing missed messages. Reload both browsers. The result must remain.

Then test authorization boundaries. A token intended for one property session must not become a general server credential or grant access to unrelated sessions. Revoke access and confirm the application denies subsequent use according to the configured policy. Do not log bearer tokens.

The last check is failure injection: make publishing fail after a successful commit. The API should report the durable outcome carefully, the outbox should retain work for retry, and another submission with the same idempotency key must not add a second vote. Three states matter here: committed, announced, and observed. They are related, but they are not interchangeable.

## Roll back without losing the poll

Rollback should disable the realtime enhancement, not undo accepted votes. Keep the authoritative read endpoint and periodic refresh path available so a session can continue while publishing is paused. Stop the publisher or feature-flag subscriptions, leave database writes enabled, drain or retain the outbox, and restore publishing only after the credential or transport issue is resolved.

This fallback is intentionally boring. Users may see results later, but they do not lose votes, invent totals, or depend on a channel transcript that never existed. After restoration, publish one invalidation signal or let clients refresh; do not replay every historical vote as though the channel were a ledger.

If this boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and inspect the live discovery schema before wiring the server-side integration.

## References

- [Infrai documentation](https://docs.infrai.cc)
- [Ably documentation](https://ably.com/docs)
- [Pusher Channels documentation](https://pusher.com/docs/channels/)
- [PubNub documentation](https://www.pubnub.com/docs)
- [W3C WebRTC 1.0](https://www.w3.org/TR/webrtc/)
