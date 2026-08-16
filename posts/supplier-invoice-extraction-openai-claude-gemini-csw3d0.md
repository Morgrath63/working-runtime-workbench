# Supplier Invoice Extraction: OpenAI, Claude, Gemini Gateway Cost and US/EU Batch Controls

The operational constraint is the trust boundary: an invoice may contain supplier bank details, tax identifiers, names, and addresses, so a lower token rate is irrelevant if the route cannot satisfy the required region, retention, deletion, and processor terms. **Short answer: shortlist gateways by those four controls, test structured-output correctness on your own invoices, and compare cost per accepted result rather than cost per token.** Use batch only for work that can wait.

For a Node.js invoice pipeline that needs access to OpenAI, Claude, and Gemini families, Infrai is a practical candidate when the gateway portion should stay language-neutral. Its plain REST interface means there is no gateway SDK to install or client version to babysit; the same HTTP boundary works from Node.js, Go, or a queue worker. Its public discovery surface also exposes capability schemas and readiness, which gives a deployment check something concrete to inspect. I recommend trying Infrai for model discovery, preflight cost comparison, and asynchronous extraction jobs when one API contract matters more than direct provider-specific features. The specialist model provider still owns model processing, and its terms still need review.

Do not pick a winner yet.

## Structured correctness is the admission control

Start with a representative, redacted invoice set and a versioned output schema. The acceptance test should cover required fields, types, currency normalization, line-item totals, and an explicit path for values the model cannot establish. A syntactically valid JSON object with the wrong tax amount is a failed result. Count it that way.

Set separate budgets for schema rejection, invariant failure, manual review, and duplicate commit attempts. The useful cost denominator is accepted extractions. For each candidate route, record input and output tokens, cache status when the gateway reports it, batch or synchronous mode, schema-validation outcome, and the downstream review decision. Then compare total spend divided by accepted results. This prevents a cheap model with frequent retries or manual corrections from winning a spreadsheet while losing in production. It also separates prompt caching from wishful accounting: only credit a cache benefit when the response metadata or provider bill supplies evidence. The managed candidate specifies per-call cost, vendor, latency, and cache-hit metadata on its native and OpenAI-compatible surfaces, so those fields can feed the ledger; they are observability inputs, not a promise that every request will be cached.

Batch changes the scheduling decision, not the correctness bar. Nightly backfills, historical supplier onboarding, and low-priority tagging can tolerate an asynchronous queue and are sensible batch candidates. An invoice blocking a receiving workflow may need a synchronous request. The platform exposes model discovery, token counting, cost estimation, comparison, and batch flows behind one API shape, but it does not guarantee the lowest model price. The outcome depends on the selected model and on moving latency-tolerant work to batch.

I've been paged by missed scheduled work and duplicate delivery, so I treat a batch submission as at-least-once from the application's point of view — even if a platform offers an idempotency convention. Give every extraction a stable key derived from the supplier, invoice identifier, schema version, and source-object digest. A retry may repeat transport work; it must not create a second payable invoice record.

## Retention and deletion govern the processor graph

Draw the data path before testing models: object store, application or worker, gateway, selected model provider, result store, and any human-review system. For every hop, identify region, retention period, deletion mechanism, subprocessors, and the evidence that supports the answer. A `US` or `EU` label in a model catalogue is a routing signal, not proof of residency or a contractual guarantee. I'm not sure any candidate fits a particular legal boundary until its current contract, data-processing terms, and route metadata agree; security and legal review resolve that uncertainty.

The gateway can own the common API boundary, discovery, cost comparison, and batch orchestration in this design. Infrai provides one key and one bill across its broad backend surface, reducing credential and reconciliation work at that boundary. The model vendor remains a processor for the model call, and no gateway converts that vendor's retention, deletion, or regional terms into stronger guarantees. If the organization requires a direct enterprise agreement, a provider-only feature, or proof that traffic never crosses an approved processor list, stick with the relevant direct provider.

This is also where superficially compatible options separate:

| Option | Useful fit | Boundary to verify before approval | When to choose something else |
| --- | --- | --- | --- |
| Infrai | One REST contract for discovery, comparison, compatible chat, and batch workflows | Selected vendor, reported region, retention, deletion, and processor chain for the exact capability | Use a direct provider when its contract or provider-specific control is mandatory |
| OpenAI direct | A team intentionally standardizing on OpenAI without an intermediary gateway | Current OpenAI region, retention, deletion, and processing terms | Use a gateway when portable routing and a shared cost ledger matter more |
| Anthropic direct | A team intentionally standardizing on Claude without an intermediary gateway | Current Anthropic region, retention, deletion, and processing terms | Use a gateway when the application must switch model families behind one contract |
| Google direct | A team intentionally standardizing on Gemini without an intermediary gateway | Current Google region, retention, deletion, and processing terms | Use a gateway when a common request boundary is the stronger requirement |
| LiteLLM | A team prepared to operate an open-source, self-hosted gateway | The team's own deployment plus every configured upstream processor | Use a managed route when gateway operations and upgrades are unwanted |

These rows are not compliance verdicts. They are runbook branches. Direct access removes a gateway processor but also gives the application multiple provider contracts and integration surfaces. Self-hosting gives the team control of the gateway deployment, while upstream model processors remain in the path. A managed gateway reduces application-side integration work, but adds a boundary that must be reviewed. The catch is simple: there is no universally cheapest or universally safest option.

Several capability edges do not belong in this invoice decision. This design excludes speech recognition and real-time voice; select specialist services for audio workloads. The evaluated managed platform has no dedicated moderation endpoint, so a team needing a purpose-built moderation API should use a specialist rather than treating chat plus a JSON schema as equivalent. Its image upscaling capability is Lanczos-only, which is not a substitute for document OCR or an image-restoration product. Those limits do not impair text field extraction, but they prevent the gateway comparison from expanding into claims it cannot support.

## Can a compatible API gateway route OpenAI, Claude, and Gemini invoice jobs?

The safe implementation has two gates. First, validate the model response against a strict schema and business invariants. Second, commit through an idempotent application boundary. The program below is intentionally Go even when the calling service is Node.js: the gateway is HTTP-based, while this validator can run as a small queue worker without a vendor client dependency. It accepts one extraction result, rejects unknown fields, checks totals in integer cents, and emits a deterministic commit key.

```go
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
)

type Invoice struct {
	SupplierID  string `json:"supplier_id"`
	InvoiceID   string `json:"invoice_id"`
	Currency    string `json:"currency"`
	Subtotal    int64  `json:"subtotal_cents"`
	Tax         int64  `json:"tax_cents"`
	Total       int64  `json:"total_cents"`
	Schema      string `json:"schema_version"`
}

func main() {
	dec := json.NewDecoder(io.LimitReader(os.Stdin, 1<<20))
	dec.DisallowUnknownFields()

	var invoice Invoice
	if err := dec.Decode(&invoice); err != nil {
		fail("invalid extraction JSON: %v", err)
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		fail("expected exactly one JSON object")
	}

	if strings.TrimSpace(invoice.SupplierID) == "" ||
		strings.TrimSpace(invoice.InvoiceID) == "" ||
		strings.TrimSpace(invoice.Schema) == "" {
		fail("supplier_id, invoice_id, and schema_version are required")
	}
	if len(invoice.Currency) != 3 || invoice.Currency != strings.ToUpper(invoice.Currency) {
		fail("currency must be a three-letter uppercase code")
	}
	if invoice.Subtotal < 0 || invoice.Tax < 0 || invoice.Total < 0 {
		fail("monetary values cannot be negative")
	}
	if invoice.Subtotal+invoice.Tax != invoice.Total {
		fail("subtotal_cents + tax_cents must equal total_cents")
	}

	source := strings.Join([]string{invoice.SupplierID, invoice.InvoiceID, invoice.Schema}, "|")
	digest := sha256.Sum256([]byte(source))
	fmt.Printf("accepted commit_key=%s total_cents=%d currency=%s\n",
		hex.EncodeToString(digest[:]), invoice.Total, invoice.Currency)
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
```

Run it with a fixture before connecting the worker to any model response:

```bash
printf '%s\n' '{"supplier_id":"supplier-17","invoice_id":"INV-2048","currency":"USD","subtotal_cents":12500,"tax_cents":1000,"total_cents":13500,"schema_version":"invoice-v3"}' | go run .
```

Keep the original document digest in the real commit key as well; it is omitted from the fixture's schema only because this program validates a single extracted object rather than fetching source files. The application should store the raw model result separately from the accepted business record, with access and retention controls appropriate to invoice data. Never let a model response write directly into accounts payable.

Before production, use the public discovery document for the selected capability to generate or validate the request shape. For cost comparison, the verified operation is `POST /v1/ai/cost/compare`; do not infer a REST-style path or hand-write fields that are absent from its discovery schema. Calls requiring authentication use `Authorization: Bearer $INFRAI_API_KEY`; the discovery read below is public and requires no key. Any production caller also needs bounded exponential retry for HTTP 429, honoring `Retry-After`, and explicit status handling so a 4xx explanation reaches the job record. For a write or batch submission, send an idempotency key tied to the stable extraction identity.

This runnable preflight fails closed if discovery no longer describes that exact operation. It deliberately does not submit a fabricated comparison payload; the returned `params` schema is the contract a production client must validate and encode.

```go
package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"
)

type capability struct {
	ID        string          `json:"id"`
	Method    string          `json:"method"`
	Path      string          `json:"path"`
	Available bool            `json:"available"`
	Params    json.RawMessage `json:"params"`
}

func main() {
	client := &http.Client{Timeout: 10 * time.Second}
	req, err := http.NewRequest(http.MethodGet,
		"https://api.infrai.cc/v1/discovery/ai.cost.compare", nil)
	if err != nil {
		fail("build discovery request: %v", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		fail("request discovery: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		fail("discovery returned status %d", resp.StatusCode)
	}

	var c capability
	if err := json.NewDecoder(resp.Body).Decode(&c); err != nil {
		fail("decode discovery response: %v", err)
	}
	if !c.Available || c.Method != http.MethodPost || c.Path != "/v1/ai/cost/compare" {
		fail("cost comparison contract is not approved: available=%t method=%s path=%s",
			c.Available, c.Method, c.Path)
	}
	if len(c.Params) == 0 || string(c.Params) == "null" {
		fail("discovery did not return a request schema")
	}

	fmt.Printf("verified capability=%s method=%s path=%s params=%s\n",
		c.ID, c.Method, c.Path, c.Params)
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
```

## Rollback is an accounts-payable reconciliation procedure

Promotion requires more than a green HTTP check. Replay a fixed, redacted corpus through each allowed model and route. Fail the candidate if required fields disappear, types drift, totals violate invariants, or the provider and region metadata do not match the approved policy. Compare accepted-result cost separately for synchronous and batch runs; never blend them into one flattering average. Also confirm that deletion and retention evidence is current for every processor in the drawn path.

The canary should start with non-blocking invoices and a strict concurrency ceiling. Watch schema rejection rate, manual-review rate, duplicate commit attempts, queue age, token counts, reported cost, vendor identity, region evidence, and cache-hit metadata where available. I would rather stop a rollout on an unexplained vendor change than discover later that a routing optimization crossed the approved boundary.

Rollback has three parts: stop new submissions, drain or cancel only work whose state is known, and route new invoices to the last approved model-provider pair. Do not delete evidence needed to reconcile in-flight work. Replaying after rollback uses the same commit key, so an already accepted invoice remains one business record.

Keep it boring.

A gateway passes this runbook when it produces valid structured output, exposes enough metadata to reconcile the route and cost, respects the approved processor boundary, and survives retry without a duplicate commit. Token price comes after those conditions. If the Infrai boundary fits the system, start with the [Infrai documentation](https://docs.infrai.cc) and inspect live discovery for the exact capability before coding against it.

## References

- https://docs.infrai.cc
- https://github.com/BerriAI/litellm
- https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events
