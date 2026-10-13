# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · CI/CD lab – start here
# MAGIC
# MAGIC **Goal (≈ 55 min):** experience the CI/CD loop with a tiny, ready-made bundle – entirely inside the Databricks workspace:
# MAGIC
# MAGIC ```
# MAGIC   change code ──► test ──► deploy to DEV ──► run & check ──► deploy to PROD
# MAGIC        ▲                                                         │
# MAGIC        └───────────────────── next change ◄──────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC Today **you** click *Deploy*. In real life a CI/CD pipeline (GitHub Actions / Azure DevOps) does exactly the same steps after a pull request is merged – step 5 shows how.
# MAGIC
# MAGIC > ℹ️ *Declarative Automation Bundles* = new name (March 2026) of *Databricks Asset Bundles (DABs)*. Same `databricks.yml`, same `databricks bundle …` commands.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agenda
# MAGIC
# MAGIC | Step | Notebook | What you do | Time |
# MAGIC |---|---|---|---|
# MAGIC | 0 | `00_start_here` | get your own copy of the bundle | 5 min |
# MAGIC | 1 | `01_explore_the_bundle` | read `databricks.yml` and the job YAML | 8 min |
# MAGIC | 2 | `02_deploy_and_run_dev` | validate + deploy to **dev**, run the job | 10 min |
# MAGIC | 3 | `03_change_and_redeploy` | the CI/CD loop: change → test → redeploy | 15 min |
# MAGIC | 4 | `04_promote_to_prod` | deploy the same bundle to **prod** | 8 min |
# MAGIC | 5 | `05_real_cicd_pipeline` | what a real pipeline does (GitHub Actions / Azure DevOps) + clean-up | 10 min |

# COMMAND ----------

# MAGIC %md
# MAGIC ## What's in the box
# MAGIC
# MAGIC ```
# MAGIC cicd_lab_simple/
# MAGIC ├── guide/            <- these instructions
# MAGIC ├── bundle/           <- ⭐ the bundle (the thing you deploy)
# MAGIC │   ├── databricks.yml          bundle name, variables, targets dev + prod
# MAGIC │   ├── resources/cicd_lab_job.yml   the job: bronze → silver → gold
# MAGIC │   ├── src/                    3 notebooks + order_rules.py (pure Python rules) + 04_quality_check
# MAGIC │   ├── tests/                  unit tests for order_rules.py + run_tests notebook
# MAGIC │   └── pytest.ini              how CI runs the tests
# MAGIC ├── solutions/step3/  <- the files after step 3, if you get stuck
# MAGIC ├── ci_reference/     <- GitHub Actions + Azure DevOps pipeline YAML (read-only, step 5)
# MAGIC ├── tools/            <- get_my_bundle (only for the "shared folder" setup, see below)
# MAGIC └── trainer/          <- trainer only
# MAGIC ```
# MAGIC
# MAGIC The job is self-contained: it generates 300 "Danube Goods" orders itself – no files, no volumes, nothing to break.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0 · Get your own bundle folder (5 min)
# MAGIC
# MAGIC Bundles can only be deployed from the workspace UI when they live in a **Git folder**. Your trainer tells you which of the two ways applies:
# MAGIC
# MAGIC ### Way A – your own Git folder (default)
# MAGIC
# MAGIC 1. Left sidebar → **Workspace** → your home folder (`/Users/<you>`).
# MAGIC 2. **Create** → **Git folder**.
# MAGIC 3. **Git repository URL:** the URL your trainer gives you → the folder name fills in automatically → **Create Git folder**.
# MAGIC 4. Your bundle is `<your Git folder>/bundle/`. You work **only** there.
# MAGIC
# MAGIC ### Way B – shared Git folder (if your trainer says so)
# MAGIC
# MAGIC 1. Open `tools/get_my_bundle` in the shared folder → **Run all** (Serverless).
# MAGIC 2. Click the link at the bottom → `participants/<your_user_name>/`. That is **your** bundle – work **only** there.
# MAGIC 3. 🚫 Do not use the Git menu (pull / commit / branch) in the shared folder – it affects everybody.
# MAGIC
# MAGIC ✅ **Checkpoint:** you see a folder with `databricks.yml`, `resources/`, `src/`, `tests/` that belongs to you.
# MAGIC
# MAGIC 🆘 **If it fails:** *Create → Git folder* is missing or the clone fails → tell your trainer (Git URL allow-list / permissions) and use Way B.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Words you need today
# MAGIC
# MAGIC | Term | Meaning |
# MAGIC |---|---|
# MAGIC | **bundle** | a folder with a `databricks.yml` at its root |
# MAGIC | **target** | *where/how* to deploy: `dev`, `prod` |
# MAGIC | **validate** | check the YAML + resolve variables – nothing is changed in the workspace |
# MAGIC | **deploy** | create/update everything so that the workspace matches the YAML |
# MAGIC | **CI** (continuous integration) | every change is automatically **tested + validated** before it is merged |
# MAGIC | **CD** (continuous delivery/deployment) | every merged change is automatically **deployed** – first dev/test, then prod |
# MAGIC
# MAGIC ➡️ Continue with `01_explore_the_bundle`.
