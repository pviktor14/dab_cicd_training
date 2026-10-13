# Databricks notebook source
# MAGIC %md
# MAGIC # 🧪 Run the unit tests in the workspace
# MAGIC
# MAGIC In CI the pipeline runs `pytest` on a build agent. Here we do the same by hand: import `tests/test_order_rules.py` and call every `test_…` function.
# MAGIC No Spark, no cluster state – these tests take milliseconds.
# MAGIC
# MAGIC **Run all.** ✅ All green = the rules still do what the tests say. ❌ = the last cell fails and tells you which test broke.

# COMMAND ----------

import os, sys, importlib, traceback

_nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_tests_dir = os.path.dirname(_nb_path)
if not _tests_dir.startswith("/Workspace"):
    _tests_dir = "/Workspace" + _tests_dir
_src_dir = os.path.join(os.path.dirname(_tests_dir), "src")

for _p in (_src_dir, _tests_dir):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import order_rules, test_order_rules
importlib.reload(order_rules)        # pick up your latest edits
importlib.reload(test_order_rules)

print("rules :", order_rules.__file__)
print("tests :", test_order_rules.__file__)

# COMMAND ----------

results = []
for name in sorted(n for n in dir(test_order_rules) if n.startswith("test_")):
    try:
        getattr(test_order_rules, name)()
        results.append((name, "✅ passed", ""))
    except Exception as e:
        results.append((name, "❌ FAILED", f"{type(e).__name__}: {e}"))
        traceback.print_exc(limit=-1)   # shows the failing assert line

for name, status, detail in results:
    print(f"{status}  {name}  {detail}")

failed = [r[0] for r in results if r[1] != "✅ passed"]
assert not failed, f"{len(failed)} test(s) failed: {failed}"
print(f"\nAll {len(results)} tests passed.")