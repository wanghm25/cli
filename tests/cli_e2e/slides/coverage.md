# Slides CLI E2E Coverage

## Metrics
- Denominator: 4 leaf commands
- Covered: 3
- Coverage: 75.0%

## Summary
- TestSlides_CreateWorkflowAsUser: proves the user slides workflow through `create presentation with slide as user` and `get created presentation xml as user`; creates a fresh presentation, asserts returned IDs, then reads back the XML content to prove the title and slide body persisted.
- TestSlidesAddSlideDryRunE2E / TestSlidesDeleteSlideDryRunE2E: dry-run only. They pin the request shapes the unit tests cover, but through the real binary, which is the only layer that proves a full `<slide>` XML document survives flag parsing with its quotes and angle brackets intact. Delete additionally proves the shortcut runs without `--yes`, unlike the high-risk-write raw command.
- Blocked area: `slides +media-upload` is still uncovered because it needs a deterministic local image fixture plus XML follow-up proof that is separate from the base create/read workflow.
- Not attempted live: `+add-slide` / `+delete-slide` write paths against a real presentation. They mutate a shared deck and delete is only recoverable through history revert, so live coverage would need a throwaway presentation created and torn down per run.

## Command Table

| Status | Cmd | Type | Testcase | Key parameter shapes | Notes / uncovered reason |
| --- | --- | --- | --- | --- | --- |
| ✓ | slides +create | shortcut | slides_create_workflow_test.go::TestSlides_CreateWorkflowAsUser/create presentation with slide as user | `--title`; `--slides ["<slide ...>"]` | read back through raw slides API to prove persisted XML |
| ✓ | slides +add-slide | shortcut | slides_slide_add_delete_dryrun_test.go::TestSlidesAddSlideDryRunE2E | `--slide "<slide ...>"`; `--before-slide-id`; `--revision-id`; `--tid` | dry-run only; live write path not covered |
| ✓ | slides +delete-slide | shortcut | slides_slide_add_delete_dryrun_test.go::TestSlidesDeleteSlideDryRunE2E, TestSlidesDeleteSlideWikiDryRunE2E | `--slide-id`; `--tid`; wiki URL | dry-run only; destructive live path not covered |
| ✕ | slides +media-upload | shortcut |  | none | needs a stable local image fixture plus follow-up slide XML proof |
