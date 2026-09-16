# Accepted direction and validation fix — 16 September 2026

Historical preparation below. The prompt has since been tested; see
[the revised one-pass evaluation](ONE_PASS_V2_RESULTS.md) for current results
and remaining integration requirements.

The user agreed to retain the independently testable page agent and use **one
combined page request as the integration baseline**. Specialist requests remain
an option for targeted follow-up tests, rather than an always-on routing stage.
The main crawler has not yet been integrated with this page interface.

## Implemented

Company research 0.15.3 fixes the phone evidence validator. Numeric comparison now
applies only to the phone value; the owner name still requires quoted textual
evidence. Previously a valid name such as Peter Grabe failed because it contained
no digits. Conversely, a false owner named Studio 12 could pass because the phone
contained 12. Both cases were reproduced with failing tests before the change.

The tests also retain rejection of unquoted owners, wrong phone numbers and
assignment of a switchboard number to an unsupported person. Source presence
still does not independently prove that the quoted owner/value belong together.
This is a narrow fix, not new phone normalization or relaxed actor validation.

The original seven-page outputs were replayed locally using their unchanged
model documents and HTML. No LLM calls or new page fetches were made.

| Source-presence replay | One pass | Routed |
|---|---:|---:|
| Records, unchanged | 255 | 336 |
| Source matched before | 75 | 189 |
| Source matched after | 81 | 202 |
| Phone records with erroneous owner hold removed | 6 | 13 |

All 19 changed records retained their values, owners, quotations and IDs.
Non-phone results were unchanged, no new issues appeared, and hashes confirmed
that all 146 original experiment files remained untouched. These are corrected
source-check outcomes, not new extraction or an overall accuracy measure.

Artifacts: data/phone-validation-20260916/report.json and the per-page results
beside it. Copies of the changed implementation and the next controls are saved
in that directory.
The [validation archive receipt](VALIDATION_RECEIPT.json) records a verified
backup of the replay artifacts and frozen changed implementation.

## Prepared for the next model test

The lab prompt now gives explicit boundaries and examples for:

- actor/job evidence and source-stated claim dates;
- company profiles versus people, and addresses versus market presence;
- specific technologies versus DOM names, bare hosts and profile links;
- planned adoption versus deployment, and service expertise versus product sales;
- explicit corporate ties versus hosting/employee/geographical hints;
- individual documents versus report collections, order forms and routine policies;
- scored navigation links versus facts available on the current page.

Future fixture preparation uses controls version 0.2: 40 positive checks with the
two known source-name matching corrections, and 19 negative checks rather than
four. Original frozen controls and original metrics remain unchanged.

Applying the expanded controls to the **old** outputs gives 40/40 selected
identities for both, and 15/19 negative checks passed for one-pass versus 4/19
for routed. This post-hoc baseline captures the errors that motivated the changes;
it is not evidence that the revised prompt has fixed them. See
expanded-control-baseline.json in the replay directory.

The new prompt has **not been tested with a model**. Next run a bounded one-pass
evaluation with these controls, inspect any remaining taxonomy/attribution errors,
then integrate queue scheduling and final merge only after page-level results
are satisfactory. Missing actor quotations and invisible characters in email
labels remain separate validation work.

## Verification

146 regular package tests and six lab tests passed. Four opt-in browser tests
were skipped; fetching/browser behavior was not changed. Ruff, formatting and
type checks passed. Source and wheel builds succeeded for 0.15.3.
No deployment, catalog/backend submission or new paid model run was performed.
