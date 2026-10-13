# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · What a real CI/CD pipeline does (10 min)
# MAGIC
# MAGIC Today you were the pipeline: you ran the tests, clicked *Deploy*, picked the target. In a real project **nobody** deploys to prod by hand. The same steps run automatically:
# MAGIC
# MAGIC ```
# MAGIC  feature branch ──► pull request ──► CI ─────────────────────► merge to main ──► CD ───────────────────────────► prod
# MAGIC  (your Git folder)   (review by a    • pytest (unit tests)                       • approval gate (optional)
# MAGIC                       colleague)     • bundle validate -t dev/prod               • bundle deploy -t prod  (as service principal)
# MAGIC                                      • (optional) deploy + run in a test target  • bundle run  (smoke test)
# MAGIC ```
# MAGIC
# MAGIC | Today (lab) | Real life |
# MAGIC |---|---|
# MAGIC | you edit in your Git folder | you edit on a **feature branch** (Git folder or IDE) and open a **pull request** |
# MAGIC | you run `tests/run_tests` | **CI** runs `pytest` on every pull request – red = no merge |
# MAGIC | the Deploy dialog validates | **CI** runs `databricks bundle validate` for every target |
# MAGIC | you click Deploy → prod | **CD** runs `databricks bundle deploy -t prod` after the merge |
# MAGIC | prod runs as you | prod runs as a **service principal** (`run_as`) – not tied to a person who may leave |
# MAGIC | you are logged in | the pipeline logs in as a service principal via **OIDC / workload identity federation** – no secret stored |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Read the two reference pipelines (5 min)
# MAGIC
# MAGIC Open `ci_reference/` (next to `bundle/`). Both files do the same thing – pick the one your team uses:
# MAGIC
# MAGIC | File | Platform |
# MAGIC |---|---|
# MAGIC | `github-actions-bundle-ci-cd.yml` | GitHub Actions (`databricks/setup-cli` action, `DATABRICKS_AUTH_TYPE: github-oidc`) |
# MAGIC | `azure-pipelines-bundle-ci-cd.yml` | Azure DevOps (`AzureCLI@2` task with a workload-identity-federation service connection) |
# MAGIC
# MAGIC Find in the file:
# MAGIC
# MAGIC 1. Which event starts **CI**, which starts **CD**?
# MAGIC 2. Which command would have caught your red test from step 3?
# MAGIC 3. Where is the **manual approval** before prod configured?
# MAGIC 4. Which secret/password is stored in the pipeline?
# MAGIC
# MAGIC <details><summary>Answers</summary>
# MAGIC
# MAGIC 1. CI: pull request to `main` (and every push to `main`). CD: push/merge to `main` only (`if:` / `condition:`).
# MAGIC 2. `pytest` – it runs `tests/test_order_rules.py`, exactly like `tests/run_tests` did.
# MAGIC 3. Not in the YAML: on the GitHub **environment** `prod` (*Required reviewers*) or the Azure DevOps **environment** `databricks-prod` (*Approvals and checks*).
# MAGIC 4. None – both use OIDC / workload identity federation. GitHub stores only the SP's *client ID* (not secret); Azure DevOps uses the service connection.
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. What would change in `databricks.yml` for a real prod (2 min)
# MAGIC
# MAGIC ```yaml
# MAGIC   prod:
# MAGIC     mode: production
# MAGIC     workspace:
# MAGIC       host: https://adb-<prod-workspace>.azuredatabricks.net      # often a separate prod workspace
# MAGIC       root_path: /Workspace/Shared/.bundle/${bundle.name}/prod     # one prod, not one per user
# MAGIC     git:
# MAGIC       branch: main                                                 # refuse deploys from other branches
# MAGIC     run_as:
# MAGIC       service_principal_name: <application-id-of-prod-SP>
# MAGIC     permissions:
# MAGIC       - level: CAN_VIEW
# MAGIC         group_name: users
# MAGIC     variables:
# MAGIC       catalog: allianz_prod
# MAGIC       schema: sales
# MAGIC ```
# MAGIC
# MAGIC No `name_prefix` – there is only **one** prod. The `[prod <you>]` prefix exists today only because 14 people share one workspace.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Clean up (3 min)
# MAGIC
# MAGIC The **prod** job is scheduled daily at 06:00 – don't leave it running after the training.
# MAGIC
# MAGIC **Option 1 – your trainer cleans up** (default): the trainer notebook removes all `cicd_lab` jobs and `cicd_*` schemas after the course.
# MAGIC
# MAGIC **Option 2 – yourself, web terminal** (if your trainer shows it): attach any notebook to Serverless → compute drop-down → **Web terminal**, then
# MAGIC
# MAGIC ```bash
# MAGIC cd /Workspace/Users/<you>/<your Git folder>/bundle      # Way B: …/participants/<you>
# MAGIC databricks bundle destroy -t prod
# MAGIC databricks bundle destroy -t dev
# MAGIC ```
# MAGIC
# MAGIC `destroy` removes the jobs and the deployment folder – not your source files, and not the tables (the schemas were created by the notebooks, not by the bundle).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎓 Recap
# MAGIC
# MAGIC | Principle | Where you saw it |
# MAGIC |---|---|
# MAGIC | everything is code | job, schedule, parameters, targets in YAML – next to the notebooks |
# MAGIC | test before deploy | `order_rules.py` + `tests/` – the red test in step 3 |
# MAGIC | same artefact, different environments | one bundle → `dev` and `prod` targets, only variables differ |
# MAGIC | prod is immutable | prod runs a copy; it only changes through a deploy |
# MAGIC | automate the loop | `ci_reference/` – PR → CI → merge → CD as a service principal |