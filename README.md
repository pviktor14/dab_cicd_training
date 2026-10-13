# cicd_lab_simple – CI/CD with Declarative Automation Bundles (45–60 min)

Hands-on for the Day 2 module **"CI/CD as a core activity"**. A deliberately small replacement for the big `04.2 - dab_lab`:
one ready-made bundle, one job (bronze → silver → gold), two targets (`dev`, `prod`), one tested Python module.
Participants do not write a bundle from scratch – they **operate the CI/CD loop**: change → test → deploy dev → run → deploy prod.
Everything happens **inside the Databricks workspace** (serverless, no laptop CLI). The real pipelines (GitHub Actions / Azure DevOps) are shown as reference YAML.

> *Declarative Automation Bundles* = new name (March 2026) of *Databricks Asset Bundles*. Same YAML, same CLI.

## Contents

| Path | For | What |
|---|---|---|
| `guide/00_start_here` … `05_real_cicd_pipeline` | participants | step-by-step notebooks with ✅ checkpoints and 🆘 hints |
| `bundle/` | participants | **the bundle**: `databricks.yml`, `resources/cicd_lab_job.yml`, `src/` (3 notebooks + `order_rules.py` + `04_quality_check`), `tests/` (pytest file + `run_tests` notebook), `pytest.ini` |
| `solutions/step3/` | participants | the three files edited in step 3 |
| `ci_reference/` | everybody (read-only) | `github-actions-bundle-ci-cd.yml`, `azure-pipelines-bundle-ci-cd.yml` – PR → tests + validate → merge → deploy prod as service principal (OIDC) |
| `tools/get_my_bundle` | participants, Way B only | copies `bundle/` to `participants/<user>/` |
| `trainer/cleanup` | trainer | removes jobs, schemas and `.bundle/cicd_lab` folders after the course |

The job is **self-contained**: `01_bronze` generates 300 orders with a fixed seed – no volumes, no files, no dependency on Day 1.
Expected numbers (for checking): 300 bronze rows, 287 silver rows, 179 `large` orders (→ 93 after the change in step 3).

| | dev | prod |
|---|---|---|
| Job | `[dev <user>] cicd_lab_job` | `[prod <user>] cicd_lab_job` |
| Schema | `allianz_training.cicd_<user>_dev` | `allianz_training.cicd_<user>_prod` |
| Schedule (daily 06:00 Europe/Berlin) | paused (`mode: development`) | **active** |
| Code | source-linked to the participant's Git folder | copied to `~/.bundle/cicd_lab/prod/files` |
| Runs as | participant | participant (`run_as: user_name`) – real life: service principal |

No service principal, no second catalog, no extra grants beyond `CREATE SCHEMA` – that is what keeps it simple.

## Timing (≈ 55 min)

| Min | Step | Trainer notes |
|---|---|---|
| 0–5 | 00 Get your bundle folder | Way A or B (below). Have the Git URL on the projector. |
| 5–13 | 01 Explore | Walk through `databricks.yml` on the projector – dev vs prod table is the key slide. |
| 13–23 | 02 Deploy + run dev | Demo the 🚀 Deployments pane once. First serverless run takes 2–4 min – use the wait for "where did the deployment go". |
| 23–38 | 03 Change → test → redeploy | The red test is intentional – sell it as "CI just saved you". Code change = no deploy (source-linked), YAML change = deploy. |
| 38–46 | 04 Promote to prod | Stress: in real life only the pipeline deploys prod. |
| 46–56 | 05 Real pipeline | Show `ci_reference/` side by side; questions; clean-up. |

Short on time: make step 4 a trainer demo, skip the optional parts (⭐ / "prove that prod is frozen").

## Trainer prep checklist

**Workspace / Unity Catalog**
- [ ] Serverless compute and workspace files enabled (requirements for bundles in the workspace).
- [ ] Participants (training1…14@abylon.io) have `USE CATALOG` + `CREATE SCHEMA` on `allianz_training` (the job creates `cicd_<user>_dev` / `_prod` itself).

**Get the material into the workspace – pick ONE way**

*Way A – every participant clones their own Git folder (recommended, simplest isolation)*
- [ ] Push the content of this folder (root = this README) to a **public** GitHub repo. Public repos can be cloned into a Git folder **without Git credentials**. Nothing secret in here.
- [ ] Check that the workspace admin settings allow that Git URL (Git URL allow-list, if configured).
- [ ] Put the URL into your slide / chat. Participants: Workspace → home → **Create → Git folder** → URL → **Create Git folder**.

*Way B – one shared Git folder (if a public repo is not possible)*
- [ ] Push to any repo (Azure DevOps / GitHub, private is fine), create **one** Git folder under your home (**Create → Git folder**).
- [ ] Share it with the participants group: **Can Edit** (if *Deploy* is greyed out for participants in the dry run: **Can Manage**).
- [ ] Do **not** add `participants/` to `.gitignore` (bundle sync respects `.gitignore` – ignored files would not be deployed to prod).
- [ ] Tell participants: run `tools/get_my_bundle`, never use the Git menu in the shared folder.

**Dry run (strongly recommended, ~20 min, as one training user, e.g. training14)**
- [ ] Step 0: Git folder clone works (Way A) / `get_my_bundle` copies 10 items (Way B).
- [ ] Step 2: 🚀 Deployments pane shows targets **dev** and **prod**; deploy dev works; job name `[dev training14] cicd_lab_job`, schedule paused.
- [ ] Step 2: the job run is green – in particular `silver` can `import order_rules` (module next to the notebook). Numbers 287 / 179.
- [ ] Step 3: `tests/run_tests` runs (no pytest needed); a code edit is picked up by the next dev run **without** a deploy (source-linked); after the YAML edit + deploy the job has 4 tasks.
- [ ] Step 4: **prod deploy works from the UI for a non-admin user** (`mode: production` + explicit `root_path` + `run_as: user_name` + `permissions`). Job `[prod training14] cicd_lab_job`, schedule active, task notebook path under `.bundle/cicd_lab/prod/files/`.
- [ ] Step 5 (only if you want to show it): web terminal opens on serverless and `databricks bundle destroy -t dev` works from the bundle folder.
- [ ] `trainer/cleanup` → `list`, then `delete`.

## After the training

Run `trainer/cleanup` (`list` → `delete`). The **prod jobs are scheduled daily at 06:00** – don't skip this.
It deletes jobs tagged `course: cicd_lab_simple`, schemas `allianz_training.cicd_*_dev|prod` and `/Users/*/.bundle/cicd_lab`. Git folders / `participants/*` stay.

## Run the unit tests locally / in CI

```bash
cd bundle
pip install pytest
pytest          # 5 passed
```

## Docs this lab is based on (verified Oct 2026)

- Bundles in the workspace – requirements (Git folder, serverless, workspace files): https://learn.microsoft.com/azure/databricks/dev-tools/bundles/workspace
- Deploy bundles and run workflows from the workspace (Deployments pane, Deploy dialog, Bundle resources, source-linked deployments): https://learn.microsoft.com/azure/databricks/dev-tools/bundles/workspace-deploy
- Deployment modes (development / production, presets): https://learn.microsoft.com/azure/databricks/dev-tools/bundles/deployment-modes
- GitHub Actions (`databricks/setup-cli`, `github-oidc`): https://learn.microsoft.com/azure/databricks/dev-tools/ci-cd/github
- Azure DevOps authentication (`AzureCLI@2`, workload identity federation): https://learn.microsoft.com/azure/databricks/dev-tools/auth/auth-with-azure-devops
- CI/CD overview: https://learn.microsoft.com/azure/databricks/dev-tools/ci-cd/
