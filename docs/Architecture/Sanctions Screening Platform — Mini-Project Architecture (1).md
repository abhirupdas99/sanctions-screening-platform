# Sanctions Screening Platform — Mini-Project Architecture

Oct 30, 2026 · @Abhirup

## Purpose and scope

We will build one working sanctions-screening pipeline, at roughly 1/10,000 of bank scale, in which each of the five take-home tasks is a real stage rather than a standalone exercise. Every customer file is checked on arrival, cleaned, deduplicated and compared with yesterday. Only new or changed customers are screened, alerts are decided by simulated analysts, and the results feed management and access reports.

| Take-home task | Stage in the product | Why the bank needs it |
| --- | --- | --- |
| Task 3: checksum validation | Landing gate for every inbound file | A corrupted or truncated file means customers go unscreened |
| Task 1: dedupe and parent | Silver layer, before screening | One person should mean one alert and one decision |
| Task 4: full vs delta | Silver layer, change detection | Only new or changed customers are rescreened each day |
| Task 2: monthly alert activity | Gold layer, management information | Shows compliance management what happened to alerts |
| Task 5: user access report | Gold layer, governance | Feeds the quarterly review of who can clear alerts |

| Component | Real in our build | Simulated |
| --- | --- | --- |
| Oracle database | Yes (Docker, Oracle 26ai Free) | Holds the screening tool's own tables, as Firco's back end would |
| Lakehouse (bronze, silver, gold) | Yes (PySpark + Delta, then Databricks) | Data volume: hundreds of customers, not millions |
| Sanctions list | Real public list (Canada SEMA, XML) | Refreshed manually, not by a vendor feed |
| Firco | No: it's licensed software | A Python "mock Firco" with fuzzy name matching, alerts and an audit log |
| Analysts and the 4-eyes review | No | A script that makes Level 1 and Level 2 decisions |
| Orchestration | Databricks Workflows (or a local runner) | The 02:00 nightly schedule is triggered by hand |

Out of scope: real customer data, real-time payment messages (SWIFT / ISO 20022), and filing to FINTRAC or OSFI. The design shows where each would plug in.

## How sanctions screening works at a Canadian bank

A Canadian bank must not deal with, or provide financial services to, any listed person. It therefore screens every customer and every payment against government lists, and reports any listed person's property it holds to FINTRAC immediately.

**The legal basis**

| Item | What it requires | Source |
| --- | --- | --- |
| Sanctions laws | The United Nations Act, the Special Economic Measures Act (SEMA) and the Justice for Victims of Corrupt Foreign Officials Act (Magnitsky) prohibit dealings and financial services for listed persons, including indirect ones | [Global Affairs Canada](https://www.international.gc.ca/world-monde/international_relations-relations_internationales/sanctions/financial-sector-secteur-financier.aspx?lang=eng) |
| Lists to screen | The UN Security Council Consolidated List and the Consolidated Canadian Autonomous Sanctions List (published as XML) | [Global Affairs Canada](https://www.international.gc.ca/world-monde/international_relations-relations_internationales/sanctions/consolidated-consolide.aspx?lang=eng) |
| Listed Person or Entity Property Report | Report property held for a listed person to FINTRAC "immediately", disclose it to the RCMP or CSIS, don't complete the transaction, keep records for 5 years. SEMA and Magnitsky lists were added on 1 Oct 2025 | [FINTRAC](https://fintrac-canafe.canada.ca/guidance-directives/transaction-operation/Guide5/5-eng) |
| Suspicious transaction reports | Suspected sanctions evasion is reportable (since Aug 2024) | [Global Affairs Canada](https://www.international.gc.ca/world-monde/international_relations-relations_internationales/sanctions/financial-sector-secteur-financier.aspx?lang=eng) |
| OSFI monthly reports | Federally regulated institutions file forms OSFI-525 and OSFI-590 monthly for names under the Magnitsky regulations and the Criminal Code list of entities | [OSFI](https://www.osfi-bsif.gc.ca/Eng/fi-if/amlc-clrpc/snc/Pages/default.aspx) |

**Two kinds of screening run side by side**

- **Customer (name) screening, in batch.** Customers are screened at onboarding, then rescreened whenever their details change, which is what the Task 4 delta finds. They're also rescreened whenever the list changes, because a customer who was clean yesterday may be listed today. A list update triggers a rescreen of the whole customer base against the new names only.
- **Payment screening, in real time.** Each outgoing or incoming wire is screened before release. A possible hit holds the payment until an analyst decides, so screening sits inside the payment's critical path.

**Where Firco fits.** LexisNexis Firco Continuity is the screening engine. It filters at high volume using more than 40 tunable matching algorithms, runs a configurable alert-review workflow with multiple-user validation (4-eyes) and a "blind mode", and keeps a full audit trail so the bank can answer regulators' questions about past decisions. It runs in the cloud or on premises and is API-first ([LexisNexis brochure](https://risk.lexisnexis.com/-/media/files/product%20pages/brochure/lnrs-firco-continuity-brochure2023-nxr15974-00-0423-en-us.pdf)). Firco Compliance Link adds case management and ISO 20022 support ([LexisNexis](https://risk.lexisnexis.com/global/en/products/firco-compliance-link)).

The data engineer's job is everything around the engine: deliver clean, complete, deduplicated input on time, prove nothing was lost, and turn the engine's alert and audit tables into reporting. The engineer never makes or changes an alert decision. That separation is segregation of duties.

## Target architecture

&#91;embedded content: target architecture · sources, lakehouse, screening engine\]

Files flow down through the lakehouse to the screening engine, and its alert tables flow back up into bronze for reporting. Only new or changed customers cross the gold-to-engine line, as a checksummed file. The way back is a nightly, read-only, incremental extract through Azure Data Factory and a self-hosted integration runtime (SHIR).

| Component | Production at the bank | In our simulation |
| --- | --- | --- |
| Landing storage | ADLS Gen2, private endpoints, files kept for 7 years | Local folder; a Unity Catalog volume in Free Edition |
| Processing | Azure Databricks jobs, Delta tables, Unity Catalog | PySpark + `delta-spark` locally, then Free Edition |
| Orchestration | Azure Data Factory (with a SHIR for Firco's Oracle) and Databricks Workflows, triggered on file arrival | A `run_night.sh` script, then a Databricks Job |
| Screening engine | Firco Continuity on its own servers, Oracle back end | Python mock Firco writing to Docker Oracle |
| Secrets and identity | Key Vault, Entra ID, service principals | `.env` file (not committed), separate Oracle users |
| Reporting | Databricks SQL or Power BI on gold tables | Databricks SQL dashboard in Free Edition |
| Monitoring | Azure Monitor, Log Analytics, on-call paging | `ctl.run_log` + a failure check that prints a red banner |

### Integration with Firco

Decision: hand Firco a file going in, and extract from it with ADF + SHIR, read-only and incrementally, each night coming out. Nothing time-critical depends on the lakehouse, so nightly is enough, and this keeps network access to the screening system as small as possible.

| Flow | Design | Why |
| --- | --- | --- |
| (A) Lakehouse → Firco | Gold extract written as a file + checksum to Firco's drop folder; Firco imports it with its own loader. The pipeline never writes to Firco's tables | Firco's tables are a regulated system's store; segregation of duties; the file is evidence of exactly what was screened |
| (B) Firco → lakehouse | ADF copy through a SHIR, read-only user on 5 tables, ideally from a Data Guard standby. Audit log incremental on `audit_event_id` (append-only); small tables copied in full. Parquet in ADLS, then a scheduled Databricks job | Production Firco untouched; outbound-only runtime; credentials in Key Vault; reproducible landed files |

**Who needs Firco data, and how fast**

| Need | Speed | Served from |
| --- | --- | --- |
| Stop a sanctioned payment | Seconds | Inside Firco |
| Report listed property to FINTRAC "immediately" | Same day | Firco's case manager, by the confirming analyst |
| Monthly alert activity (Task 2), OSFI monthly reports | Monthly | Lakehouse |
| User access review (Task 5) | Quarterly | Lakehouse |

**Options rejected**

- **Lakehouse Federation:** live queries load Firco's database during business hours, and a live report can't be reproduced for a past date.
- **Direct JDBC from Databricks:** works, but it opens network access from the lakehouse into the screening zone with no business benefit.
- **Lakeflow Connect CDC:** still in Beta, and it needs a privileged Oracle user plus archive-log changes on a vendor-managed system.
- **GoldenGate / Qlik:** licence cost for freshness nobody uses.

This decision is revisited if compliance needs an intraday view of open alerts. The first step would be hourly incremental ADF runs, not CDC.

## End-to-end data flow

The main flow is a nightly batch that must finish before the business day starts. Two side flows, list changes and payments, reuse the same screening engine.

**Nightly customer screening (the main simulation)**

1. **Land.** Each source system drops `customers_YYYYMMDD.tsv` plus its `.checksum` into the landing zone, sending the checksum last as the "file complete" signal.
2. **Validate (Task 3).** The checksum gate recomputes MD5 / SHA3. Exit code 1 stops the pipeline and quarantines the file; exit code 0 continues. A row-count control total is checked too.
3. **Bronze.** The file is loaded as-is (text columns, nothing dropped), plus `load_seq`, the file name and the load time.
4. **Silver: clean and validate (Task 1).** Matching keys are built (case and spaces normalised), bad rows are flagged `INVALID` with a reason, and good rows continue.
5. **Silver: dedupe (Task 1).** One parent per name + country is picked; duplicates point to it.
6. **Silver: change detection (Task 4).** Today's parents are compared with yesterday's snapshot, giving `NEW`, `UPDATES` or `NO_UPDATES`, plus `DELETED` for IDs that disappeared.
7. **Gold: screening extract.** Only `NEW` + `UPDATES` parents are written as a file + checksum to Firco's drop folder, and Firco imports it with its own loader. The pipeline never writes to Firco's tables.
8. **Screen (mock Firco).** Names are fuzzy-matched against the list. Hits become alerts in the engine's Oracle tables, and every action is written to the audit log.
9. **Decide (simulated analysts).** Level 1 reviews; possible matches escalate to Level 2 (4-eyes). Each decision is an audit-log row.
10. **Extract back.** The engine's Oracle tables (alert snapshot, audit log, users, profiles) are extracted each night by ADF through a SHIR: read-only, incremental on the audit log, landed as Parquet in ADLS, then loaded into bronze.
11. **Report (Tasks 2 and 5).** Gold tables build the monthly alert activity and the user access review, served through Databricks SQL.
12. **Reconcile.** Counts are proven at every hop: rows in = valid + invalid, parents + duplicates = valid, and screened = `NEW` + `UPDATES`.

**Sanctions list change (simulated weekly)**

1. Download the latest `sema-lmes.xml` and validate it.
2. Diff it against the previous list version to find newly listed names.
3. Rescreen **all** active customer parents against only the new names. This is the one time unchanged customers are screened again.

**Payment screening (designed, not built)**

Payment hub → event stream (Azure Event Hubs) → Firco real-time filter → release, or hold for review. The same alert tables and audit log receive the result, so Tasks 2 and 5 report on payments too, with no extra work.

## Data model

The lakehouse holds 15 Delta tables: 2 control tables plus bronze, silver and gold. Oracle holds the screening engine's own tables, reusing the exact Task 2 and Task 5 schemas so the take-home queries run unchanged.

| Layer | Table | Grain (one row per…) | Built by |
| --- | --- | --- | --- |
| Control | `ctl.file_manifest` | inbound file: name, checksum result, row count, status | Task 3 gate |
| Control | `ctl.run_log` | pipeline run: start, end, status, counts at each hop | Every stage |
| Bronze | `bronze.customer_raw` | input line, as received, + `load_seq`, file, load time | Ingest |
| Bronze | `bronze.sanctions_list_raw` | list entry per list version | List ingest |
| Bronze | `bronze.firco_*` (4 tables) | nightly copy of the engine's Oracle tables | Extract back |
| Silver | `silver.customer_validated` | input line + matching keys + `validation_error` | Task 1 |
| Silver | `silver.customer_parent` | input line + `status` + `duplicate_of` | Task 1 |
| Silver | `silver.customer_snapshot` | parent customer per business date (Delta time travel = "full") | Task 4 |
| Silver | `silver.customer_change` | parent per date: `NEW` / `UPDATES` / `NO_UPDATES` / `DELETED` | Task 4 |
| Gold | `gold.screening_extract` | customer sent to screening that day | Step 7 |
| Gold | `gold.alert_activity_monthly` | month × actor × action × decision | Task 2 |
| Gold | `gold.user_access_review` | user × profile, with `mapping_status` | Task 5 |

**Oracle (the engine's side, schema `AML_FFF`)**

| Table | Role | Origin |
| --- | --- | --- |
| `alert_snapshot` | Current state per alert: overwritten | Task 2 schema |
| `alert_audit_log` | Every event on every alert: append-only | Task 2 schema |
| `fff_users`, `fff_user_profile`, `fof_audit_profile` | Analysts, role assignments and role-name history | Task 5 schema |
| `screening_input` | Customers the mock Firco is asked to screen | New |
| `sanctions_list_entry` | The list version the engine screens against | New |

One rule ties it together: silver and gold are rebuilt from bronze, and bronze is never edited. Any report can be reproduced for a past date, which is what an auditor asks for.

## Controls

In a bank, a pipeline that works isn't enough. It must prove it worked, and show who could change what. These controls are built into the simulation as the code is written, not added afterwards.

| Control | What we build | What it proves |
| --- | --- | --- |
| File integrity | Task 3 gate + quarantine folder; the checksum file is the completion trigger | No corrupted or half-written file is screened |
| Completeness | Reconciliation at every hop, written to `ctl.run_log`; a mismatch fails the run | No customer silently dropped between landing and screening |
| Data quality | `INVALID` rows kept with a reason; DQ counts per run; threshold alert if invalid > 1% | Bad data is visible and owned, and never blocks good data |
| Lineage | Every row carries source file, `load_seq` and run id; Unity Catalog lineage between tables | Any alert traces back to the exact input line |
| Reproducibility | Bronze is append-only; Delta time travel; reports take a business date | A past month's report can be rebuilt exactly |
| Audit trail | The engine's audit log is append-only; analysts' decisions are never updated in place | Who decided what, and when |
| Segregation of duties | The pipeline's service principal can read alert tables but not write decisions; analysts can't edit pipeline code | Engineers don't clear alerts; analysts don't change data |
| Access review | Task 5 report run each quarter, with sign-off recorded | Only the right people can clear alerts |
| Secrets | Database passwords in Azure Key Vault (a local `.env` in the simulation, never committed) | No credentials in code or Git |
| Least privilege | Separate Oracle users: owner schema (`NO AUTHENTICATION`), read-only extract user (5 tables, used by ADF), analyst app | A pipeline bug can't drop or change the engine's tables; the lakehouse never writes to Firco |
| Monitoring | Run-level alert on failure, late file or zero rows screened | A silent night with no screening is caught by morning |

The last row matters most. The failure that hurts banks isn't a crash, which someone notices. It's a run that "succeeds" with nothing screened.

## Simulation plan

Recommendation: build everything locally first, for free and with the same code. Run the lakehouse on Databricks Free Edition next. Rent real Azure only for one final weekend, with a spending cap.

| Tier | Runs on | Cost | What runs there | Limits that matter |
| --- | --- | --- | --- | --- |
| 0. Local | Your PC: Docker Oracle (already working) + Python, PySpark and `delta-spark` in WSL | Free | All 12 steps end to end, including mock Firco and the analyst script | No Unity Catalog or Workflows UI; you play the scheduler |
| 1. Databricks Free Edition | Databricks-hosted serverless workspace | Free | Bronze → silver → gold notebooks, Unity Catalog, a Lakeflow pipeline, a Job, a Databricks SQL dashboard | Serverless only; 5 concurrent job tasks; one pipeline per type; outbound internet limited to trusted domains; non-commercial use only ([limits](https://docs.databricks.com/aws/getting-started/free-edition-limitations)) |
| 2. Real Azure (optional) | Azure trial: ADLS Gen2, Azure Databricks, Key Vault, Azure Data Factory + a SHIR on your PC | Free trial credit, then pay-as-you-go (approximate; check current terms) | The same notebooks against ADLS, secrets from Key Vault, ADF + SHIR copying Firco's Oracle tables, an ADF trigger on file arrival | Set a budget alert first; delete the resource group the same day |

**Oracle ↔ lakehouse in each tier.** The design is the same everywhere; only the transport changes. In Tier 0 a Python script plays the SHIR: it reads the five Firco tables with the read-only user, applies the `audit_event_id` watermark and writes Parquet to `landing/firco/`. Free Edition can't reach your PC, so in Tier 1 those Parquet files are uploaded to a Unity Catalog volume. In Tier 2, ADF + a SHIR installed on your Windows PC replaces the script, and the Databricks code doesn't change.

**Mock Firco (Python, about 200 lines)**

- Loads the list from `sema-lmes.xml` (official, about 1.7 MB) into `sanctions_list_entry`.
- Matches each screening name against list names and aliases with a fuzzy score (Jaro-Winkler via `rapidfuzz`). Country and date of birth add or remove points.
- Score ≥ threshold → an alert in `alert_snapshot` + an `INITIAL_REVIEW` event in `alert_audit_log`, as the WORKFLOW actor. That's the same shape as the Task 2 fixture.
- The threshold is a config value, so we can show the false-positive trade-off: lower it and alerts go up.

**Simulated analysts.** A script logs in as users from `fff_users` and picks decisions from simple rules, for example a true seeded match becomes `CONFIRMED_MATCH` after Level 2. It writes `REVIEW_DECISION`, `COMMENT` and `ESCALATION` events. Users have profiles with renamed role names, so Task 5's trap appears naturally.

**Synthetic data (generated, never real people)**

| Dataset | Size | Deliberate problems |
| --- | --- | --- |
| Customers, day 1 | 500 rows | 10% duplicates (case and space variants), 2% invalid rows, IDs stored as text |
| Daily deltas | 30 business days | Some new, some changed, some removed customers; NULLs in compared columns |
| Seeded true matches | 5 customers | Names copied from the public list, with small spelling changes |
| Checksum files | One per file | 2 corrupted, 1 truncated, 1 uppercase digest, 1 with Windows line endings |
| List versions | 2 | The second adds names, to trigger the list-change rescreen |

The customer generator invents every person, so no real individual's data is used. The sanctions list is public. The seeded matches use public list names on purpose, so screening has true positives to find.

## Build roadmap

Seven phases, each ending in something that runs and a sentence you can say in an interview. Phases 1–5 are Tier 0 (local); 6 and 7 move it to Databricks and Azure.

1. **Foundations.** Repo layout, config, `.env`, Oracle users (owner, pipeline, analyst) and the `ctl` tables.
   - Done when: `make check` runs, and the Oracle users exist with least privilege.
   - Say: "I set up separate schema-owner, pipeline and application users, so a pipeline bug can't touch the engine's tables."
2. **Data generator.** 500 synthetic customers, 30 daily files, checksum files with planted faults, 2 list versions.
   - Done when: the files land in `landing/` with a manifest of the planted faults.
   - Say: "I generate test data with known defects, so every control has something to catch."
3. **Ingest + Task 3 gate + bronze.** Checksum validation, quarantine, row-count control, append-only bronze.
   - Done when: the 4 planted bad files are quarantined and the run log shows why.
4. **Silver: Tasks 1 and 4 in PySpark.** Validation, dedupe with parent, change detection against Delta time travel.
   - Done when: the counts reconcile for all 30 days and the PySpark results match the Oracle SQL results.
   - Say: "Same logic in Oracle SQL and Spark, cross-checked. That's how I'd migrate it."
5. **Mock Firco + analysts + Tasks 2 and 5.** File hand-off to Firco, screening, alerts, the 4-eyes script, the incremental read-only extract back, gold reports.
   - Done when: the 5 seeded true matches are all alerted and confirmed, and the monthly report reconciles to the audit log.
6. **Databricks Free Edition.** Notebooks into a Job, Unity Catalog tables with lineage, a Lakeflow pipeline for silver, a SQL dashboard.
   - Done when: one job run goes from landing to dashboard, with lineage visible in Unity Catalog.
7. **Azure deploy (optional, one weekend).** ADLS, Key Vault-backed secrets, ADF + a SHIR on your PC copying Firco's Oracle tables, an ADF trigger on file arrival; then tear it down.
   - Done when: one run works on Azure with a screenshot of the cost, and the resource group is deleted.

A steady pace is about 2 phases a week. The deck can then show a working product, not just five queries.

## Design decisions

These ten decisions define the design. Each one is a likely interview question, so the reason is stated next to it.

| # | Decision | Chosen | Rejected | Reason |
| --- | --- | --- | --- | --- |
| 1 | When to screen customers | Nightly batch on `NEW` + `UPDATES`, plus a full rescreen against new list names | Rescreen everyone daily | Same coverage at a fraction of the alerts and cost |
| 2 | File completeness | Checksum file sent last is the trigger; Task 3 gate fails the run | Fixed 02:00 schedule | A half-written file would be screened partially |
| 3 | Staging types | All columns as text in bronze, validated in silver | Typed columns at load | One bad value must not fail 3 million rows |
| 4 | Dedupe key | Name + country, case and spaces normalised only | Fuzzy name matching in dedupe | Over-merging hides people; fuzzy matching is Firco's job |
| 5 | Parent choice | Lowest numeric ID, then file order (`load_seq`) | ID as text, or no tie-break | Deterministic; text sorts "19" before "2" |
| 6 | Full vs delta | Delta time travel as "yesterday"; NULL-safe comparison | Separate full table; plain `=` | Reproducible; `=` mislabels NULLs |
| 7 | Lakehouse → Firco | File + checksum into Firco's loader | Writing to Firco's tables | Segregation of duties; evidence of what was screened |
| 8 | Firco → lakehouse | ADF + SHIR, read-only, incremental, nightly | Federation, JDBC, CDC | No time-critical consumer; least network access; lowest cost |
| 9 | Reporting source | Audit log only, never the snapshot (Task 2) | Joining current status | Past months must not change when re-run |
| 10 | Compute | Scheduled job or serverless compute, auto-terminate | All-purpose clusters | Pay only while the nightly run executes |

**Open questions**

- [ ] Does Firco's batch loader accept a file per run, or does it need a fixed file name and folder?
- [ ] Is a Data Guard standby available to extract from, or only production?
- [ ] Which sanctions list version does Firco hold today, and who approves list updates?
- [ ] How long must landed files and Parquet extracts be retained? (FINTRAC records: at least 5 years)

## Sources

- [Canadian sanctions guidance — Financial sector](https://www.international.gc.ca/world-monde/international_relations-relations_internationales/sanctions/financial-sector-secteur-financier.aspx?lang=eng), Global Affairs Canada
- [Consolidated Canadian Autonomous Sanctions List](https://www.international.gc.ca/world-monde/international_relations-relations_internationales/sanctions/consolidated-consolide.aspx?lang=eng), Global Affairs Canada (XML download)
- [Reporting listed person or entity property to FINTRAC](https://fintrac-canafe.canada.ca/guidance-directives/transaction-operation/Guide5/5-eng), FINTRAC
- [Reporting on AML, ATF and sanctions](https://www.osfi-bsif.gc.ca/Eng/fi-if/amlc-clrpc/snc/Pages/default.aspx), OSFI
- [Firco Continuity brochure (2023)](https://risk.lexisnexis.com/-/media/files/product%20pages/brochure/lnrs-firco-continuity-brochure2023-nxr15974-00-0423-en-us.pdf), LexisNexis Risk Solutions
- [Firco Compliance Link](https://risk.lexisnexis.com/global/en/products/firco-compliance-link), LexisNexis Risk Solutions
- [Databricks Free Edition limitations](https://docs.databricks.com/aws/getting-started/free-edition-limitations), Databricks
- [Migrate from Community Edition to Free Edition](https://learn.microsoft.com/azure/databricks/getting-started/ce-migration), Microsoft Learn
- [Canadian Consolidated Autonomous Sanctions List dataset](https://opensanctions.org/datasets/ca_dfatd_sema_sanctions), OpenSanctions (CC BY-NC 4.0; an alternative CSV/JSON source for non-commercial use)

* [Azure Data Factory Oracle connector](https://learn.microsoft.com/azure/data-factory/connector-oracle), Microsoft Learn (when a self-hosted integration runtime is required)
* [Lakeflow Connect Oracle concepts](https://learn.microsoft.com/en-us/azure/databricks/ingestion/lakeflow-connect/oracle-concepts), Microsoft Learn (Beta; LogMiner CDC)
* [Lakehouse Federation for Oracle](https://learn.microsoft.com/en-us/azure/databricks/query-federation/oracle), Microsoft Learn

Firco's internal table design and workflow details beyond the vendor pages are typical industry patterns, not confirmed product internals.
