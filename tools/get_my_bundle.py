# Databricks notebook source
# MAGIC %md
# MAGIC # 🧰 get_my_bundle – only for the "shared Git folder" setup (Way B)
# MAGIC
# MAGIC Copies `bundle/` → `participants/<your_user_name>/` inside this shared Git folder. That copy is **your** bundle: edit and deploy only there.
# MAGIC
# MAGIC * **Run all** (Serverless).
# MAGIC * Already have a folder and want a fresh copy (e.g. you broke it)? Set the widget `overwrite = yes` and run again – files that exist in `bundle/` are overwritten, your other files stay.
# MAGIC
# MAGIC ⚠️ Never deploy from `bundle/` or `solutions/` – only from `participants/<your_user_name>/`.

# COMMAND ----------

dbutils.widgets.dropdown("overwrite", "no", ["no", "yes"])
OVERWRITE = dbutils.widgets.get("overwrite") == "yes"

# COMMAND ----------

import re, posixpath
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ObjectType, ExportFormat, ImportFormat

w = WorkspaceClient()

me = w.current_user.me().user_name
SHORT = re.sub(r"[^a-z0-9]", "_", me.split("@")[0].lower())     # like ${workspace.current_user.short_name}

nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
REPO_ROOT = posixpath.dirname(posixpath.dirname(nb_path))          # .../cicd_lab_simple/tools/get_my_bundle -> .../cicd_lab_simple
SRC = f"{REPO_ROOT}/bundle"
DST = f"{REPO_ROOT}/participants/{SHORT}"

print(f"You       : {me}")
print(f"Copy from : {SRC}")
print(f"Copy to   : {DST}")

# COMMAND ----------


def exists(path):
    try:
        w.workspace.get_status(path)
        return True
    except Exception:
        return False


def copy_tree(src, dst):
    """Copy notebooks and files (.py modules, .yml, .ini) from src to dst, keeping the folder structure."""
    n = 0
    for obj in w.workspace.list(src, recursive=True):
        if obj.object_type not in (ObjectType.NOTEBOOK, ObjectType.FILE):
            continue
        rel = obj.path[len(src):].lstrip("/")
        target = posixpath.join(dst, rel)
        w.workspace.mkdirs(posixpath.dirname(target))
        if obj.object_type == ObjectType.NOTEBOOK:
            content = w.workspace.download(obj.path, format=ExportFormat.SOURCE).read()
            w.workspace.upload(target, content, format=ImportFormat.SOURCE, language=obj.language, overwrite=True)
        else:
            content = w.workspace.download(obj.path, format=ExportFormat.AUTO).read()
            w.workspace.upload(target, content, format=ImportFormat.AUTO, overwrite=True)
        print("  copied", rel)
        n += 1
    return n


# COMMAND ----------

if not exists(SRC):
    raise FileNotFoundError(f"{SRC} not found – is this notebook still inside the cicd_lab_simple Git folder?")

if exists(f"{DST}/databricks.yml") and not OVERWRITE:
    print("Your folder already exists – nothing copied. (Set overwrite = yes for a fresh copy.)")
else:
    n = copy_tree(SRC, DST)
    print(f"✅ Copied {n} item(s).")

host = w.config.host.rstrip("/")
displayHTML(f'➡️ <a href="{host}/#workspace{DST}" target="_blank">Open your bundle folder: {DST}</a>')
