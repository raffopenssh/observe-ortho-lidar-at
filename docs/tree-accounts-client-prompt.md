# Prompt for the NE server: account link for tree reports

Copy as is. Our side is live (`docs/tree-user.md → Accounts`).

```text
Link tree-report contributors to accounts on this service (contract: GET https://<this-service>/api/v1/docs/llm.txt → "Accounts").

1. Per account (e-mail key or export subscription key) derive one constant pseudonym, server-side:
   acct = hex(HMAC-SHA256(ACCT_SECRET, "acct:" + key_id))[:32]. Never send addresses or raw keys. One user with both keys: use the subscription key id.
2. On every relayed POST /api/v1/flags keep `reporter` (neg-rep) unchanged and, when signed in, add header `X-Reporter-Account: <acct>`. Signed out: omit. Expect `account: "linked"`; on `account_warning` log, never retry or block the report.
3. Accounting: GET /api/v1/flags/account?account=<acct>&since=<unix>&until=<unix> with X-Reporter-Key → {linked, reporters, reports, tree_versions, species, measures, added, retracted, trees, flags, first_ts, last_ts}. Counts only; cache ≥ 1 h per account, fetch on demand (billing run / account page), never per page view.
4. Account deleted: DELETE /api/v1/flags/account?account=<acct>.
5. Past reports need no backfill: the first signed-in report links that browser's whole history.
Test with a mock for all three calls.
```
