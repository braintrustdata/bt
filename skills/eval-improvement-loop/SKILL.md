---
name: eval-improvement-loop
description: Autonomously run Braintrust evals locally and iterate on prompts, models, or application code to improve eval scores. Use when asked to improve eval results, optimize against an eval, repeatedly run evals, or start an eval improvement loop.
---

# Eval Improvement Loop

Run a bounded, evidence-driven loop: establish a baseline, change one hypothesis, run the eval locally, keep validated gains, revert regressions, and repeat.

## Non-negotiable rules

- Run eval code locally with `bt eval --jsonl`. Add `--local` only for offline/no-upload mode; Braintrust hill climbing requires uploaded experiments and must not use `--local`. Neither mode prevents model-provider calls or their cost.
- Treat eval files, scorers, test cases, datasets, fixtures, and expected outputs as the benchmark. **Do not edit them to raise the score.** Only edit the system under test unless the user explicitly asks to improve eval design.
- Never expose hidden/held-out expected outputs to the system under test.
- A sampled run is a smoke/development result, never a final result. Validate every kept improvement on the full dataset.
- Change one coherent idea per iteration so results are attributable.
- Work only on a dedicated `eval-loop/...` branch. Every baseline and candidate state that reaches an eval must be committed first so results map to an exact git revision.
- Preserve user work. Never use broad `git reset --hard`, `git checkout -- .`, or `git clean -fd` commands.
- Stop at the agreed iteration/cost/time limit, on user interruption, or when several structurally different ideas fail to improve the full eval. Do not run an unbounded cost-bearing loop.

## Setup

### 1. Inspect and classify

Find evals and inspect the relevant code:

```bash
bt eval --local --list <eval-paths>
git status --short
```

Determine:

- eval files/evaluator filters to run;
- the system-under-test files that may be edited;
- frozen benchmark files that must not be edited;
- primary score or metric and whether higher/lower is better;
- secondary scores and correctness checks;
- iteration, time, or cost budget.

Infer these when unambiguous. Ask only for decisions that materially change the experiment. Default to 20 iterations when the user requests autonomous iteration but gives no limit. If model calls have unknown or potentially high cost, confirm the budget before starting.

There are two distinct goals:

1. **Score optimization** (default): freeze the eval and improve the system under test.
2. **Eval design**: improve datasets/scorers/eval coverage. Define an independent meta-evaluation first; never claim that changing a scorer improved the system's score.

For score optimization, choose one execution mode and record it in `session.md`:

- **Offline**: `bt eval --local --jsonl`. Tasks and scorers run, but no immutable Braintrust experiment is uploaded.
- **Experiment**: `bt eval --jsonl`. The code runs locally and uploads an immutable experiment snapshot for comparison and sharing.
- **Hill climb**: the eval uses `BaseExperiment()` / `BaseExperiment(...)` and comparative scorers such as `Battle` or `Summary`. Run without `--local`; the previous experiment outputs must be available in Braintrust.

Do not silently switch modes during a session.

### 2. Configure Braintrust run identity (uploaded modes)

Git metadata is the authoritative link between an uploaded Braintrust experiment and the evaluated candidate, but **do not assume it is being collected**. Before the first uploaded experiment or hill-climb run, tell the user:

> Braintrust uses commit and branch metadata for experiment comparison and hill-climbing base selection when it is available. Collection depends on the SDK version and the organization's **Settings → Logging** policy; some current SDK versions (for example, Python v0.23+) may collect nothing when no organization policy is configured.

Ask the user to verify the policy or approve a one-time evaluator configuration. Follow the current [git metadata collection guidance](https://www.braintrust.dev/docs/kb/running-evaluations-per-git-commit-sha) and request only `commit`, `branch`, and `dirty` through the SDK's `git_metadata_settings` / equivalent. Call-site settings can narrow the organization policy but cannot expand beyond it. Do not claim an experiment is SHA-linked until collection is confirmed. Every uploaded eval must run from the committed candidate with `dirty: false`.

Optionally add stable Braintrust experiment metadata during this one-time setup:

```typescript
metadata: {
  eval_loop: true,
  eval_loop_session: "eval-loop/<session-slug>",
  eval_loop_mode: "experiment", // or "hill_climb"
}
```

```python
metadata={
    "eval_loop": True,
    "eval_loop_session": "eval-loop/<session-slug>",
    "eval_loop_mode": "experiment",  # or "hill_climb"
}
```

This is a provenance-only exception to the frozen-eval rule: get approval, make the metadata/git-collection change before the baseline, commit it, and then freeze the eval harness. Preserve useful existing metadata such as model and prompt version. Do not add a changing run number or candidate SHA by editing the eval file every iteration; git metadata already identifies the candidate, and `.bt/eval-loop/log.jsonl` maps that SHA to the deterministic run number and hypothesis. If the evaluator already exposes provenance as parameters, passing optional run metadata via `bt eval --param` is acceptable as long as the parameter values are recorded by the experiment and remain outside the task/scorer inputs.

Offline `--local` runs upload nothing, so Braintrust metadata does not apply; rely on the local JSONL log.

### 3. Protect the working tree

Require a Git repository and a clean tree. If there are user changes, ask the user to commit/stash them or approve a separate worktree. Do not overwrite them.

Always create a dedicated branch before the baseline. Never run the loop directly on the default branch:

```bash
git switch -c eval-loop/<short-goal>-<YYYYMMDD>
git status --short
git rev-parse HEAD
```

If setup requires legitimate changes to the system under test, commit them before the baseline. The baseline must always resolve to a commit. On resume, continue only if already on the session branch and the worktree is clean.

Store loop state under `.bt/eval-loop/` so it survives candidate reverts:

```text
.bt/eval-loop/session.md
.bt/eval-loop/log.jsonl
.bt/eval-loop/runs/<run-id>.jsonl
.bt/eval-loop/runs/<run-id>.stderr
```

Write `session.md` with the objective, execution mode, exact commands, primary score, direction, editable files, frozen files, checks, budget, branch, baseline commit, whether git metadata collection was confirmed, and any optional Braintrust metadata keys. A fresh agent should be able to resume from it. Keep `.bt/eval-loop/` out of commits; if `.bt/` is not already ignored, add this exact session directory to `.git/info/exclude` rather than changing the project's `.gitignore`.

### 4. Choose run commands

Use machine-readable output and save every run. In offline mode:

```bash
bt eval --local --jsonl <eval-paths> >.bt/eval-loop/runs/<run-id>.jsonl \
  2>.bt/eval-loop/runs/<run-id>.stderr
```

In uploaded experiment or hill-climb mode, omit `--local`:

```bash
bt eval --jsonl <eval-paths> >.bt/eval-loop/runs/<run-id>.jsonl \
  2>.bt/eval-loop/runs/<run-id>.stderr
```

For development runs on a large dataset, use a deterministic sample, again including `--local` only in offline mode:

```bash
bt eval [--local] --jsonl --sample <N> --sample-seed <SEED> <eval-paths> \
  >.bt/eval-loop/runs/<run-id>.jsonl \
  2>.bt/eval-loop/runs/<run-id>.stderr
```

Use `--filter`, `--param`, `--matrix-param`, `--runner`, or `--language` when the session requires them. Keep parameters identical between a candidate and the baseline it is compared with. Do not use `--watch`: each candidate needs a discrete output file and commit association. Respect eval-level trial counts; for stochastic tasks, multiple trials or repeated runs provide a stronger signal than a single noisy score.

`--jsonl` emits one summary object per evaluator, though eval console output may also be present. Do not parse or write loop JSONL by hand. Use the bundled helper, resolved relative to this skill:

```bash
EVAL_LOOP_LOG="<SKILL_DIR>/scripts/eval_loop_log.py"
python3 "$EVAL_LOOP_LOG" extract \
  --input .bt/eval-loop/runs/<run-id>.jsonl
```

The helper ignores non-JSON console lines, validates summary shapes, and normalizes all scores and metrics. If the primary name occurs in multiple evaluator summaries, pass `--evaluator` when appending; the helper rejects ambiguous selection instead of averaging unrelated results.

## Establish baselines

1. Run the full eval once. This is the final baseline.
2. If using sampled development runs, run the exact sample command on the unchanged baseline too.
3. For noisy evals, repeat the baseline and use the median. Record the observed noise; do not treat movement within that range as a gain.
4. Run correctness checks once before iterating.
5. Generate and append the baseline record with the helper:

```bash
python3 "$EVAL_LOOP_LOG" append \
  --log .bt/eval-loop/log.jsonl \
  --eval-output .bt/eval-loop/runs/1-full.jsonl \
  --run 1 --kind baseline --scope full --status keep --mode offline \
  --primary-name quality --primary-kind score --direction higher \
  --hypothesis "unchanged baseline" \
  --base-commit "$(git rev-parse --short HEAD)"
```

Replace `--mode offline` with `experiment` or `hill_climb` when applicable. For a sampled record, use `--scope sample --sample-count <N> --sample-seed <SEED>`. For a crash with no summary, omit `--eval-output` and `--primary-value`; null primary values are accepted only for `crash` and `checks_failed` records.

## Iteration loop

Repeat until the budget or stopping rule is reached:

1. Read `session.md`, recent `log.jsonl` entries, and `git log`.
2. Form one specific hypothesis based on failures or score patterns.
3. Record the current clean `HEAD` as `BASE_COMMIT`.
4. Edit only allowed system-under-test files.
5. Verify frozen benchmark files are unchanged and run fast syntax/type/unit checks.
6. Commit the candidate **before** evaluating it:

   ```bash
   git add -- <exact editable paths>
   git commit -m "eval-loop: <hypothesis>"
   CANDIDATE_COMMIT="$(git rev-parse HEAD)"
   ```

7. Run the deterministic sampled eval, or the full eval if it is already small.
8. Parse and compare the primary plus all secondary scores against the matching baseline/current best.
9. If a sample improves beyond noise, log it as `advance` and run correctness checks plus the **full eval on the same candidate commit**.
10. Keep only if the full primary score improves beyond noise and guardrails pass. The candidate commit remains on the branch.
11. On `discard`, `crash`, or `checks_failed`, restore the previous tree with a targeted revert commit:

    ```bash
    git revert --no-edit "$CANDIDATE_COMMIT"
    REVERT_COMMIT="$(git rev-parse HEAD)"
    ```

12. Generate the run record only after the keep/revert decision. Every candidate record includes `--commit "$CANDIDATE_COMMIT"`; failed/discarded records also include `--revert-commit "$REVERT_COMMIT"`.
13. Confirm the worktree is clean, then update `session.md` with durable wins, dead ends, and the next best ideas.

Generate every candidate record with `append`, for example:

```bash
python3 "$EVAL_LOOP_LOG" append \
  --log .bt/eval-loop/log.jsonl \
  --eval-output .bt/eval-loop/runs/4-full.jsonl \
  --run 4 --kind candidate --scope full --status discard --mode offline \
  --primary-name quality --primary-kind score --direction higher \
  --hypothesis "shorten the system prompt" \
  --changed-file src/prompt.ts --base-commit abcdef0 \
  --commit fedcba9 --revert-commit 1234567 \
  --reason "full score regressed despite a sampled gain" \
  --next "try structured instructions without removing examples"
```

`append` validates the complete existing log before writing, emits compact one-object-per-line JSON, sorts/deduplicates changed files, preserves uploaded experiment identity from the eval summary, and refuses invalid status/value/commit combinations. Run an explicit final check before reporting:

```bash
python3 "$EVAL_LOOP_LOG" validate --log .bt/eval-loop/log.jsonl
```

Do not amend or rewrite a candidate commit after evaluating it: that would change the SHA associated with the result. Keep metrics in `.bt/eval-loop/log.jsonl`; the pre-eval commit message should describe only the hypothesis.

## Braintrust hill climbing

Follow the current [Run evaluations](https://www.braintrust.dev/docs/evaluate/run-evaluations) guidance when the eval uses `BaseExperiment`:

- Braintrust uses git metadata when available (timestamps otherwise) to select the best base experiment, so the dedicated branch and pre-eval candidate commits are mandatory.
- `BaseExperiment()` automatically builds expected values from the prior experiment, merging reviewed `expected` values with its outputs; reviewed expected values take precedence. `BaseExperiment(name="...")` pins a specific base. Never manually copy or expose those values to the task.
- Use a **comparative scorer** such as `Battle` or `Summary` to decide whether the candidate beats its direct base. A comparative score above 50% means it wins on average.
- Also track at least one **non-comparative scorer** such as `ClosedQA`. It provides an absolute quality signal that remains comparable across non-sequential experiments.
- Do not interpret a comparative score as globally comparable across unrelated bases. Log the comparison experiment name and immutable experiment identity extracted by the helper.
- Keep a candidate only when it beats the direct base, non-comparative guardrails do not regress materially, and full-run/check requirements pass. The kept experiment becomes the next hill-climbing reference.
- Hill climbing cannot run in offline `--local` mode because the base experiment outputs must be retrieved and the new immutable experiment must be stored.

## Comparison policy

- **Higher-is-better:** candidate must be greater than the best full result plus the noise/minimum-improvement threshold.
- **Lower-is-better:** candidate must be less than the best full result minus that threshold.
- Re-run marginal gains before keeping them.
- Do not average unrelated scores unless the user explicitly defines that aggregate.
- A catastrophic secondary regression, correctness failure, or policy/safety regression blocks a keep even when the primary improves.
- Periodically run a different deterministic confirmation sample to detect overfitting, but compare it with a baseline from the same seed.

## Safe revert procedure

Evaluated candidates are always commits. Revert a failed candidate with `git revert --no-edit <candidate-commit>` so the exact evaluated revision remains in history and no broad working-tree reset can destroy user work. If an idea fails before it is committed/evaluated, restore only its exact tracked paths and remove only untracked files created by that attempt.

Never revert or commit `.bt/eval-loop/`; it is the persistent experiment record. Before the next iteration, verify the branch tree matches the last kept commit and `git status --short` is clean.

## Resume and finish

When `.bt/eval-loop/session.md` exists, read it, the tail of `log.jsonl`, current status, and recent commits before continuing. Re-run the current best if the environment or dependencies changed.

At the end report:

- baseline and best **full** primary score with absolute/relative change;
- secondary score changes and checks;
- kept commits and files changed;
- discarded hypotheses and useful findings;
- whether the limit, plateau rule, or user interruption stopped the loop.
