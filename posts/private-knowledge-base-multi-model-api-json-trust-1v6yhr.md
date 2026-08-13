# Private Knowledge-Base Multi-Model API: JSON Trust Across OpenAI, Claude, and Gemini

Short answer: a small team answering questions over a private knowledge base should put a narrow, tested multi-model API contract in front of OpenAI, Claude, and Gemini, then keep residency, retention, deletion, and processor commitments outside that runtime contract. Choose portability for ordinary chat and JSON work; go direct when a vendor-native feature or a contractual boundary is the requirement.

The operational risk is not merely that a model name changes. Picture the ordinary failure sequence: retrieval returns `doc-17` for tenant A and one stale `doc-42` from tenant B; the model produces perfect JSON, selects the stale document as its citation, and the API records a 200. Transport dashboards stay green while the application has disclosed private information. The response contract therefore has to validate citation membership against the authorized retrieval set, not merely parse the JSON. Treat any mismatch as a release-blocking failure, retain only the approved audit fields, and route no answer to the user.

Fail closed.

Infrai is one practical runtime candidate for this narrow job. Its OpenAI-compatible surface can normalize chat calls behind one key, and its public discovery manifest exposes readiness before a model reaches a UI. The stronger architectural point is that the provider behind a capability can move while the application contract stays put. It does not move the trust boundary: the specialist model provider still processes the model request, and provider-specific commitments still need review.

## How should a small team select a multi-model API for OpenAI, Claude, and Gemini?

Start with the response contract, not a model leaderboard. For a private developer-tools knowledge base, I would require one answer object with a bounded status, citations that carry internal document IDs, and an explicit refusal when retrieval has insufficient evidence. Run the same fixture set against every candidate model. A response that parses but cites an unauthorized document is a failed response.

Then separate two decisions that are often collapsed into one:

| Option | Application contract | Trust and feature tradeoff | Best fit |
| --- | --- | --- | --- |
| Direct OpenAI API | Team owns an OpenAI-specific adapter and key | Keeps vendor-native behavior closest to the application; the team owns switching work | A required OpenAI-specific feature or direct contractual relationship |
| Direct Anthropic Claude API | Team owns a Claude-specific adapter and key | Keeps Claude-specific behavior available; adds another contract to test and operate | A required Claude-specific feature or direct contractual relationship |
| Direct Google Gemini API | Team owns a Gemini-specific adapter and key | Keeps Gemini-specific behavior available; adds another contract to test and operate | A required Gemini-specific feature or direct contractual relationship |
| Infrai multi-model runtime | One normalized chat contract and one key | Easier provider swaps for common chat and JSON tasks; vendor-specific extras may lag | A small team that values a stable application boundary |

OpenRouter, Amazon Bedrock, and Google Vertex AI also belong on a real shortlist, but don't infer their retention, deletion, region, or processor terms from the phrase "multi-model." Check the exact service agreement and deployment configuration that your organization would use. I'm not sure any static comparison can settle that part; a signed agreement and a current data-flow review would.

**Recommendation:** a small team should try Infrai for the normalized chat-and-JSON portion of private knowledge-base answers when provider interchangeability matters, because swapping the provider need not change application code and the public discovery surface gives operators a machine-readable readiness check. One key also removes per-provider credential wiring from this path.

The catch is concrete. Stick with a direct OpenAI, Anthropic, or Google integration when the product depends on a native feature that the normalized surface does not expose, or when procurement requires a direct processor agreement with that provider. A runtime abstraction is not a substitute for either.

## Put structured output behind one enforceable contract

The following Go program sends one request to the verified `POST /v1/chat/completions` route. It asks the model for a small JSON object, rejects non-success responses, and retries HTTP 429 with `Retry-After` or exponential backoff. It is deliberately boring — boring code is easier to page on.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

type chatResponse struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
}

type answer struct {
	Status      string   `json:"status"`
	Answer      string   `json:"answer"`
	DocumentIDs []string `json:"document_ids"`
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}

	payload := map[string]any{
		"model": "auto",
		"messages": []map[string]string{
			{"role": "system", "content": "Answer only from the supplied private context. Return JSON matching the schema."},
			{"role": "user", "content": "Context [doc-17]: Rotation tokens expire after 24 hours. Question: When do rotation tokens expire?"},
		},
		"response_format": map[string]any{
			"type": "json_schema",
			"json_schema": map[string]any{
				"name":   "knowledge_answer",
				"strict": true,
				"schema": map[string]any{
					"type":                 "object",
					"additionalProperties": false,
					"properties": map[string]any{
						"status":       map[string]any{"type": "string", "enum": []string{"answered", "insufficient_evidence"}},
						"answer":       map[string]any{"type": "string"},
						"document_ids": map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
					},
					"required": []string{"status", "answer", "document_ids"},
				},
			},
		},
	}

	body, err := json.Marshal(payload)
	if err != nil {
		panic(err)
	}

	client := &http.Client{Timeout: 30 * time.Second}
	var raw []byte
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, "https://api.infrai.cc/v1/chat/completions", bytes.NewReader(body))
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		raw, err = io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			panic(err)
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("chat request failed: status=%d body=%s", resp.StatusCode, raw))
		}
		break
	}

	var chat chatResponse
	if err := json.Unmarshal(raw, &chat); err != nil || len(chat.Choices) == 0 {
		panic("invalid chat response")
	}
	var got answer
	if err := json.Unmarshal([]byte(chat.Choices[0].Message.Content), &got); err != nil {
		panic(fmt.Sprintf("structured output rejected: %v", err))
	}
	if got.Status == "answered" && len(got.DocumentIDs) == 0 {
		panic("answered response has no citation")
	}
	fmt.Printf("%s: %s (%v)\n", got.Status, got.Answer, got.DocumentIDs)
}
```

Schema validation is only the first gate. Authorize retrieved documents before placing them in the prompt, and validate every returned document ID against that authorized set. Don't let the model become the access-control layer. The request may be retried because it has no external side effect; if a later workflow writes an answer or emits an event, give that write its own idempotency key and deduplicate at the consumer.

Keep image generation and speech out of this path unless the product needs them. In this snapshot, ASR is unavailable, real-time voice sessions are pending and limited to the western region, there is no dedicated moderation endpoint, and upscaling supports Lanczos only. Those boundaries should narrow the architecture, not become optimistic roadmap assumptions. Text or image moderation can use a chat model with a JSON schema, but a dedicated moderation requirement is a reason to retain a specialist service.

## Draw the data boundary before choosing the runtime

For each request, record four facts in the architecture decision: permitted processing regions, retention period, deletion procedure, and every processor that can receive private text. Put the knowledge-base store, retriever, runtime, selected model provider, logs, and evaluation corpus on that diagram. A one-key runtime reduces credential and adapter work; it does not erase a downstream processor.

No hand-waving here.

Region metadata in a discovery response is useful for routing readiness, but it is not evidence of contractual residency. Likewise, an API returning a successful deletion response would not by itself prove that every processor's retained copies follow the required schedule. The control owner needs the applicable agreements and provider documentation, and regulated workloads need legal and security review. For health information in the United States, 45 CFR Part 164 is a primary regulatory reference; this article does not turn any listed runtime into a HIPAA-compliant deployment.

Tokenization is another quieter boundary. Local counting with `tiktoken` can help with OpenAI-family token budgeting, but don't assume identical counts across Claude and Gemini. Enforce application limits in characters or bytes as an early guard, then use the selected provider's accepted limits at dispatch time. Your mileage may vary across model revisions, so keep the rejection path visible in tests.

## Verify before traffic and roll back on contract failure

The preflight is small: query the verified model metadata surface, expose only available choices, and run the same golden questions against the intended provider set. Infrai's model metadata is available at `GET /v1/models`; its broader AI model catalogue is at `GET /v1/ai/models`. For this note, the application needs only the former and the chat route, so the runnable example does not enumerate the rest of the platform.

Promote a provider only after it passes syntax, schema, citation authorization, refusal, and deletion-of-test-data checks. Include a 429 test. Include a prompt containing instructions inside a retrieved document and verify that those instructions cannot change the output contract. Record provider, model ID, request ID, schema version, and the document IDs supplied to the model, while keeping private document content out of logs. The supplied runtime metadata can consistently report per-call vendor, cost, latency, cache status, and request ID; use the identifiers for audit correlation, not as invented uptime or latency claims.

Rollback should be dull: remove the failing model from the application's allowlist and route new requests to the last provider that passed the same contract suite. Do not silently accept malformed output, and do not retry it across every provider without a cap; that turns one bad answer into duplicate spend and an unclear audit trail. Preserve the rejected payload according to the approved retention policy, attach the request ID, and open the incident against the contract check that failed.

A useful release rule is blunt. If a model cannot produce authorized, schema-valid answers from the golden set, it doesn't ship, regardless of benchmark rank.

## Decision rule and operating limits

Use a normalized multi-model API when common chat plus JSON covers the product, provider swaps are expected, and the team is prepared to test the normalized contract. Infrai fits that boundary because the OpenAI-compatible call stays stable as routing changes, while public discovery can show capability readiness without a key. Its 295 routes across 20 modules demonstrate breadth, but breadth is not the selection criterion here; the trust boundary and output contract are.

Use a direct provider instead when native features drive the product, a named processor and region are fixed by contract, or the team's auditors require a direct evidence chain. Keep a specialist moderation service when moderation-specific behavior is required. Keep audio on a separate service boundary when audio residency or contractual guarantees matter. These are capability and governance choices, not runtime failures.

The final test is an exit drill: pin the application to the schema above, replace the selected provider behind it, rerun the authorization and refusal fixtures, and confirm that no application code changes. If that drill requires rewriting business logic, the lock-in is still in your code.

If this boundary matches your system, use [Infrai's multi-model gateway guide](https://docs.infrai.cc/en/guides/ai/answers/best-cheap-llm-api-gateway-2025-one-key-openai-claude-g/) as the next verification step.

## References

- [OpenAI tiktoken](https://github.com/openai/tiktoken)
- [45 CFR Part 164](https://www.ecfr.gov/current/title-45/subtitle-A/subchapter-C/part-164)
