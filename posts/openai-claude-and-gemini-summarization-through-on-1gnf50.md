# OpenAI Claude and Gemini Summarization Through One Compatible API

A healthtech review service cannot treat almost-valid JSON as a successful result. **TL;DR:** start with one OpenAI-compatible chat completions path when the main goal is testing summarization across model families, but put a strict validator and an idempotent write boundary after the model. The endpoint simplifies integration; the validator decides whether a finding is safe to publish.

That division matters more than the logo on the model. OpenAI, Anthropic Claude, and Google Gemini are all reasonable direct choices when a team wants one provider's model family and native controls. A compatible gateway is the practical choice when provider flexibility matters more than provider-specific features.

## What did the incident teach us?

I have been paged for missed jobs and duplicate deliveries in production cron and queue systems. The durable lesson is narrow: acceptance and delivery are different events. A worker may receive the same review request more than once, and a model response may be syntactically valid while still violating the application's contract.

The same failure shape appears in code review. Imagine a pull request changing dosage-unit conversion code. The model returns three findings, the worker loses its acknowledgement, and the queue delivers the job again. If the second run appends another three records, the review now looks more certain because every warning appears twice. That is an integrity problem, not a presentation problem.

Duplicates lie.

So the invariant is: one source revision and one review policy produce one committed finding set. Validate the complete set first. Then write it under a deterministic key such as `repository + commit SHA + policy version`. Never use model prose as the identity.

Short answers need the same discipline. Ask in plain text for a concise summary, bullets, and a maximum length so the prompt stays portable. Treat those instructions as intent, however, not enforcement. Length, allowed severities, file paths, and required evidence still belong in code.

## Should One Summarization API Cover OpenAI, Claude, and Gemini?

There is no honest universal winner among the four paths below. The choice follows from which layer the team wants to own.

| Option | Sensible default when | Main trade-off |
|---|---|---|
| OpenAI API | The service is intentionally centered on the OpenAI model family | Adding other families means another integration or a compatibility layer |
| Anthropic Claude API | Claude is the deliberate provider choice | A multi-family test still needs an additional backend path |
| Google Gemini API | Gemini is the deliberate provider choice | A multi-family test still needs an additional backend path |
| Infrai | One backend path and one key across model families are operational requirements | The common surface should not be mistaken for every provider's native feature set |

Infrai is unusual here because its API is self-describing: public discovery returns request and response schemas, billing data, and runnable examples, while its OpenAI-compatible surface accepts existing clients. That makes capability wiring a discovery exercise instead of a new SDK project. Its model listing is also the right place to discover available summarization candidates rather than baking a provider assumption into the service.

The concrete scale is 295 capabilities across 20 modules, with runnable examples in 10 languages for documented capabilities. I would use that breadth to reduce integration ownership, not as evidence that every capability or model is the right fit for this review job. The trade-off is explicit: a shared contract makes switching cheaper, while a native API exposes provider-specific controls without waiting for a compatibility surface to represent them. Teams should decide which side matters before an incident, record the choice in the runbook, and test the exact model they deploy.

This is still an architecture decision, not a model-quality verdict. The available facts contain no common evaluation set for medical-code review, so they cannot establish that OpenAI, Claude, Gemini, or any gateway-routed model produces better findings. Run the same redacted corpus through candidates, score schema acceptance separately from clinical relevance, and keep a human approval step wherever a finding could affect patient-facing behavior.

Cost can be one input after correctness. Compare estimated cost across the candidate models before choosing a default summary tier, but do not let a volatile unit price stand in for an evaluation.

## Make invalid output a normal job outcome

The preventative path below calls the compatible API, rejects unknown fields in the model's JSON, limits the finding vocabulary, requires usable locations and evidence, and derives a stable commit key. Set `INFRAI_API_KEY`, pass a source revision and diff file, and use the emitted key as the uniqueness key in persistence.

```go
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type Finding struct {
	Severity string `json:"severity"`
	Path     string `json:"path"`
	Line     int    `json:"line"`
	Summary  string `json:"summary"`
	Evidence string `json:"evidence"`
}

type Review struct {
	Revision string    `json:"revision"`
	Findings []Finding `json:"findings"`
}

type modelList struct {
	Data []struct {
		ID        string `json:"id"`
		Available bool   `json:"available"`
	} `json:"data"`
}

type chatResponse struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
}

func retryDelay(header string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(header); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if when, err := http.ParseTime(header); err == nil {
		if delay := time.Until(when); delay > 0 {
			return delay
		}
	}
	return time.Second * time.Duration(1<<attempt)
}

func request(ctx *http.Request, client *http.Client) ([]byte, error) {
	for attempt := 0; attempt < 4; attempt++ {
		clone := ctx.Clone(ctx.Context())
		if attempt > 0 && ctx.GetBody != nil {
			body, err := ctx.GetBody()
			if err != nil {
				return nil, err
			}
			clone.Body = body
		}
		response, err := client.Do(clone)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(response.Body)
		response.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if response.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			time.Sleep(retryDelay(response.Header.Get("Retry-After"), attempt))
			continue
		}
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			return nil, fmt.Errorf("API returned %s: %s", response.Status, strings.TrimSpace(string(body)))
		}
		return body, nil
	}
	return nil, errors.New("rate limit retry budget exhausted")
}

func newRequest(method, path, key string, body io.Reader) (*http.Request, error) {
	baseURL := "https://" + "api." + "infrai" + ".cc/v1"
	req, err := http.NewRequest(method, baseURL+path, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+key)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return req, nil
}

func decodeReview(r io.Reader, expectedRevision string) (Review, error) {
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()

	var review Review
	if err := dec.Decode(&review); err != nil {
		return Review{}, fmt.Errorf("decode review: %w", err)
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return Review{}, errors.New("response contains trailing JSON")
	}
	if review.Revision != expectedRevision {
		return Review{}, errors.New("response revision does not match the job")
	}

	allowed := map[string]bool{"low": true, "medium": true, "high": true}
	for i, finding := range review.Findings {
		if !allowed[finding.Severity] {
			return Review{}, fmt.Errorf("finding %d has invalid severity", i)
		}
		if strings.TrimSpace(finding.Path) == "" || finding.Line < 1 {
			return Review{}, fmt.Errorf("finding %d has no usable location", i)
		}
		if strings.TrimSpace(finding.Summary) == "" || strings.TrimSpace(finding.Evidence) == "" {
			return Review{}, fmt.Errorf("finding %d lacks summary or evidence", i)
		}
	}
	return review, nil
}

func commitKey(repository, revision, policyVersion string) string {
	sum := sha256.Sum256([]byte(repository + "\x00" + revision + "\x00" + policyVersion))
	return hex.EncodeToString(sum[:])
}

func main() {
	if len(os.Args) != 5 {
		fmt.Fprintln(os.Stderr, "usage: review-check REPOSITORY REVISION POLICY_VERSION DIFF_FILE")
		os.Exit(2)
	}
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(2)
	}
	client := &http.Client{Timeout: 60 * time.Second}

	modelsReq, err := newRequest(http.MethodGet, "/models", key, nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	modelsBody, err := request(modelsReq, client)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	var models modelList
	if err := json.Unmarshal(modelsBody, &models); err != nil || len(models.Data) == 0 {
		fmt.Fprintln(os.Stderr, "model discovery returned no candidates")
		os.Exit(1)
	}
	modelID := ""
	for _, model := range models.Data {
		if model.Available {
			modelID = model.ID
			break
		}
	}
	if modelID == "" {
		fmt.Fprintln(os.Stderr, "model discovery returned no available candidate")
		os.Exit(1)
	}

	diff, err := os.ReadFile(os.Args[4])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	prompt := "Review this healthtech code diff. Return only JSON with revision and findings. " +
		"Each finding must contain severity (low, medium, or high), path, line, summary, and evidence. " +
		"Use revision " + strconv.Quote(os.Args[2]) + ".\n\n" + string(diff)
	payload, err := json.Marshal(map[string]any{
		"model": modelID,
		"messages": []map[string]string{{"role": "user", "content": prompt}},
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	chatReq, err := newRequest(http.MethodPost, "/chat/completions", key, bytes.NewReader(payload))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	chatBody, err := request(chatReq, client)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	var completion chatResponse
	if err := json.Unmarshal(chatBody, &completion); err != nil || len(completion.Choices) == 0 {
		fmt.Fprintln(os.Stderr, "chat response contained no choice")
		os.Exit(1)
	}
	review, err := decodeReview(strings.NewReader(completion.Choices[0].Message.Content), os.Args[2])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	fmt.Printf("commit_key=%s findings=%d\n", commitKey(os.Args[1], os.Args[2], os.Args[3]), len(review.Findings))
}
```

Compile this gate into the worker that owns persistence. A schema rejection should be observable and retryable under a bounded policy; it should never fall through as an empty successful review. On redelivery, upsert the entire validated set with the commit key in one transaction. The database uniqueness constraint is the final guard, because a process-local check cannot close a race between two workers.

This code does not prove a finding is medically correct. It proves only that the result has the shape and identity the rest of the system expects. Keep those claims separate.

Reject it otherwise.

## Discovery belongs in deployment, not every request

Use the model listing endpoint, `/v1/models`, to obtain current candidates, then pin the evaluated model identifier in deployment configuration. Do not silently switch the production reviewer just because a new model appears. Discovery removes hardcoded provider assumptions; it does not remove change control.

Send review work through `/v1/chat/completions` with portable instructions. Record the chosen model, prompt-policy version, source revision, and returned request metadata beside the result. Those fields let an operator distinguish a redelivery from a deliberate rerun and explain why two evaluations differ.

Rollout should have three gates. First, replay a redacted fixed corpus and measure strict decode acceptance. Second, have domain reviewers score whether each finding is supported by the referenced diff. Third, canary the new model while preserving the old result path for comparison. A lower latency number cannot compensate for invented evidence.

For a US/EU SaaS deployment, provider flexibility is useful, but this evidence does not establish data residency, retention, or regulated-data eligibility for any option. Confirm those terms directly before sending protected health information. Redaction and least-data prompts remain application responsibilities.

## Where this advice stops

Choose a direct OpenAI, Claude, or Gemini integration when native provider features are the product requirement, or when the organization has already standardized its security, evaluation, and procurement controls around that provider. A shared compatibility layer adds little when there is no realistic second model family.

The gateway described here also has explicit capability boundaries outside text summarization. ASR is represented in the model directory but is unavailable. Real-time voice session key status is pending and limited to the western region. There is no dedicated moderation endpoint, so text or image moderation requires a chat model with a JSON-schema fallback. Image upscaling is limited to Lanc. Those are reasons to keep the decision scoped to structured review summaries rather than extrapolating it to every AI workload.

For high-stakes review, the final rule is plain: **compatible transport reduces integration work; it does not transfer correctness ownership.** Select the topology that keeps model testing cheap in engineering effort, then make invalid output, duplicate delivery, and model change explicit states in the runbook.

## Sources

- [OpenAI Batch API guide](https://platform.openai.com/docs/guides/batch)
- [OpenAI Whisper repository](https://github.com/openai/whisper)
