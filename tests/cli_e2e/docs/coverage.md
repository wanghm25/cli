# Docs CLI E2E Coverage

## Metrics
- Denominator: 12 leaf commands
- Covered: 7
- Coverage: 58.3%

## Summary
- TestDocs_CreateAndFetchWorkflow: proves `docs +create` and `docs +fetch`; key `t.Run(...)` proof points are `create as bot` and `fetch as bot`.
- TestDocs_CreateAndFetchWorkflowAsUser: proves the same shortcut pair with UAT injection via `create as user` and `fetch as user`; creates its own Drive folder fixture first, then reads back the created doc by token.
- TestDocs_UpdateWorkflow: proves `docs +update` via `update-title-and-content as bot`, then re-fetches the same doc in `verify as bot` to assert persisted title/content changes.
- TestDocs_DryRunDefaultsToV2OpenAPI: proves `docs +create`, `docs +fetch`, and `docs +update` dry-run all emit `/open-apis/docs_ai/v1/...` requests without MCP or `--api-version` guidance; its fetch case asserts fetch sends the default `extra_param`, and its update case asserts `--reference-map` is sent as request body `reference_map`.
- TestDocs_CreateTitleDryRunPrependsContent: proves `docs +create --title` dry-run prepends an escaped `<title>...</title>` tag to request body `content`.
- TestDocsScriptCreatesUniqueTempXMLFiles, TestDocsScriptCreateTempXMLDryRunDoesNotWrite, TestDocsScriptParseXMLFromFile, TestDocsScriptParseAcceptsServerSDKAttributeAmpersand, TestDocsScriptConvertsMarkdown, TestDocsScriptConvertsSoftLineBreaksToSpaces, TestDocsScriptDryRunIsLocal, TestDocsScriptOnlineDryRunFetchesXML, and TestDocs_CreateAndFetchWorkflowAsBot/script parse by token prove `docs +script` atomically creates `<name>_<random>_folder/<name>.xml` paths under concurrency, keeps dry-run side-effect free, accepts the service SDK's bare-ampersand URL attribute form, parses local `@file` input without changing it, fetches online document URL/token input as XML, returns word/character/block profiles, and keeps local mode network-free.
- TestDocs_DryRunDefaultsToV2OpenAPI also proves `docs +history-list`, `docs +history-revert`, and `docs +history-revert-status` dry-run endpoint and query/body shapes.
- TestDocs_HistoryWorkflow proves the guarded live history flow (`LARK_DOC_HISTORY_E2E=1`): create, update, list prior revisions, revert, poll status when needed, and fetch to verify reverted content.
- Setup note: docs workflows create a Drive folder through `drive files create_folder` in `helpers_test.go`; that helper is external to the docs domain and is not counted here.
- Blocked area: media and search shortcuts still need deterministic fixtures and local file orchestration.

## Command Table

| Status | Cmd | Type | Testcase | Key parameter shapes | Notes / uncovered reason |
| --- | --- | --- | --- | --- | --- |
| ✓ | docs +create | shortcut | docs/helpers_test.go::createDocWithRetry; docs_create_fetch_test.go::TestDocs_CreateAndFetchWorkflowAsUser/create as user; docs_update_dryrun_test.go::TestDocs_DryRunDefaultsToV2OpenAPI/create; docs_update_dryrun_test.go::TestDocs_CreateTitleDryRunPrependsContent | `--parent-token`; `--doc-format markdown`; `--content`; `--title` | helper asserts returned doc id from `data.document.document_id`; dry-run asserts title is prepended into request body content |
| ✓ | docs +fetch | shortcut | docs_fetch_dryrun_test.go::TestDocsFetchDryRunIgnoresAPIVersionCompatFlag; docs_create_fetch_test.go::TestDocs_CreateAndFetchWorkflow/fetch as bot; docs_update_test.go::TestDocs_UpdateWorkflow/verify as bot; docs_create_fetch_test.go::TestDocs_CreateAndFetchWorkflowAsUser/fetch as user; docs_update_dryrun_test.go::TestDocs_DryRunDefaultsToV2OpenAPI/fetch | `--doc <docToken>`; `--doc-format markdown`; default `extra_param.enable_user_cite_reference_map=true`; `--api-version v1` compatibility flag still dry-runs the v2 fetch endpoint | |
| ✓ | docs +history-list | shortcut | docs_update_dryrun_test.go::TestDocs_DryRunDefaultsToV2OpenAPI/history list; docs_history_workflow_test.go::TestDocs_HistoryWorkflow | `--doc`; `--page-size`; `--page-token` | live workflow gated by `LARK_DOC_HISTORY_E2E=1` |
| ✓ | docs +history-revert | shortcut | docs_update_dryrun_test.go::TestDocs_DryRunDefaultsToV2OpenAPI/history revert; docs_history_workflow_test.go::TestDocs_HistoryWorkflow | `--doc`; `--history-version-id`; `--wait-timeout-ms` | live workflow gated by `LARK_DOC_HISTORY_E2E=1` |
| ✓ | docs +history-revert-status | shortcut | docs_update_dryrun_test.go::TestDocs_DryRunDefaultsToV2OpenAPI/history revert status; docs_history_workflow_test.go::TestDocs_HistoryWorkflow | `--doc`; `--task-id` | live workflow polls only when revert returns `running` |
| ✓ | docs +script | shortcut | docs_script_test.go::TestDocsScriptCreatesUniqueTempXMLFiles; docs_script_test.go::TestDocsScriptCreateTempXMLDryRunDoesNotWrite; docs_script_test.go::TestDocsScriptParseXMLFromFile; docs_script_test.go::TestDocsScriptParseAcceptsServerSDKAttributeAmpersand; docs_script_test.go::TestDocsScriptParseMarkdownFromFile; docs_script_test.go::TestDocsScriptConvertsMarkdown; docs_script_test.go::TestDocsScriptConvertsSoftLineBreaksToSpaces; docs_script_test.go::TestDocsScriptDryRunIsLocal; docs_script_test.go::TestDocsScriptOnlineDryRunFetchesXML; docs_create_fetch_test.go::TestDocs_CreateAndFetchWorkflowAsBot/script parse by token | `--command create-temp-xml --file-name <name>`; `--command parse`; `--command markdown-to-xml`; `--content @file`; `--doc <URL/token>`; local/online dry-run | atomically creates `<name>_<random>_folder/<name>.xml`, accepts service-compatible bare `&` in quoted attributes, auto-detects local XML/Markdown, fetches online input as XML for profiling, or converts Markdown to XML; local mode makes no API call |
| ✕ | docs +media-download | shortcut |  | none | no media fixture workflow yet |
| ✕ | docs +media-insert | shortcut |  | none | requires deterministic upload fixture and rollback assertions |
| ✕ | docs +media-preview | shortcut |  | none | requires deterministic media fixture |
| ✕ | docs +search | shortcut |  | none | search results are ambient and not yet stabilized for E2E |
| ✓ | docs +update | shortcut | docs_update_test.go::TestDocs_UpdateWorkflow/update-title-and-content as bot; docs_update_dryrun_test.go::TestDocs_DryRunDefaultsToV2OpenAPI/update | `--doc`; `--command overwrite`; `--doc-format markdown`; `--content`; optional `--reference-map` -> body `reference_map` | |
| ✕ | docs +whiteboard-update | shortcut |  | none | requires whiteboard fixture and DSL-specific assertions |
