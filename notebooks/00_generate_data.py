# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Generate the synthetic data (optional)
# MAGIC Writes the four CSVs straight into the `raw` volume, so nothing has to be uploaded by hand.
# MAGIC Needs the repo cloned as a **Git folder** (Workspace → Create → Git folder), so `../data` exists.
# MAGIC Same seed, same data, every time.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

import os, sys
sys.path.append(os.path.abspath("../data"))
import generate_pds_identity_data as gen

sys.argv = ["generate", "--out", RAW_PATH, "--seed", "42", "--end", "2026-11-15"]
gen.main()
display(dbutils.fs.ls(RAW_PATH))
