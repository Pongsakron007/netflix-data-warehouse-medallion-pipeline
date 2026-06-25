# 🚀 Netflix Job Workflow Configuration

This directory contains the configuration files to automatically set up the Netflix data pipeline workflow in Databricks.

## 📁 Files in This Directory

* **`netflix_workflow_config.json`** - Job workflow configuration (tasks, dependencies, settings)
* **`setup_job_workflow.py`** - Automated setup script to create the job
* **`README.md`** - This documentation file

---

## 🎯 Quick Start (For New Users)

If you've just cloned this project and want to run the Netflix pipeline:

### Step 1: Run the Setup Script

Open and run the notebook:
```
/Workspace/Users/<your-email>/Databricks-for-Data-Engineers-Bootcamp2/Netflix_project/job_configs/setup_job_workflow.py
```

This will:
- ✅ Read the job configuration from `netflix_workflow_config.json`
- ✅ Automatically adjust paths to your workspace
- ✅ Create the job workflow with 2 tasks (Bronze → Silver)
- ✅ Provide you the job URL to access it

### Step 2: Run the Workflow

1. Visit the job URL printed by the setup script
2. Click **"Run now"** to execute the pipeline
3. Monitor progress in the Runs tab

---

## 📊 Workflow Architecture

The Netflix workflow consists of **2 sequential tasks**:

```
┌─────────────────┐
│ Netflix_bronze  │  ← Task 1: Raw data ingestion to Bronze layer
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Netflix_Silver  │  ← Task 2: Data quality + SCD Type 2 to Silver layer
└─────────────────┘
```

### Task Details:

| Task Key | Notebook Path | Parameters | Purpose |
|----------|--------------|------------|----------|
| `Netflix_bronze` | `bronze_Netflix/bronze_fw_config` | `pipeline_name: netflix` | Ingest raw CSV → Bronze table with CDF |
| `Netflix_Silver` | `silver_Netflix/silver_fw_config` | `pipeline_name: netflix` | Transform Bronze → Silver (quality checks + SCD Type 2) |

---

## 🛠️ Modifying the Workflow

### Change Job Settings

Edit `netflix_workflow_config.json`:

```json
{
  "name": "Netflix_Data_Pipeline_Workflow",
  "description": "Your custom description",
  "max_concurrent_runs": 1,
  "tasks": [ ... ]
}
```

### Add a New Task

Add a new task object to the `tasks` array:

```json
{
  "task_key": "Netflix_Gold",
  "depends_on": [{"task_key": "Netflix_Silver"}],
  "notebook_task": {
    "notebook_path": "{{WORKSPACE_PATH}}/gold_Netflix/gold_fw_config",
    "base_parameters": {"pipeline_name": "netflix"},
    "source": "WORKSPACE"
  }
}
```

### Add a Schedule

Add this to the top-level of `netflix_workflow_config.json`:

```json
{
  "name": "Netflix_Data_Pipeline_Workflow",
  "schedule": {
    "quartz_cron_expression": "0 0 2 * * ?",
    "timezone_id": "America/Los_Angeles",
    "pause_status": "UNPAUSED"
  },
  ...
}
```

This schedules the job to run daily at 2 AM.

---

## 🔄 Updating an Existing Job

If you need to update a job that was already created:

**Option 1: Delete and Recreate**
1. Go to the job page in Databricks UI
2. Click **"⋮" → "Delete"**
3. Re-run `setup_job_workflow.py`

**Option 2: Manual Update**
1. Go to the job page
2. Edit tasks/settings directly in the UI
3. Export the updated config (see below)

---

## 📤 Exporting Job Configuration

If you manually modified a job in the UI and want to save the changes:

```python
import json
import requests

job_id = <your-job-id>
workspace_url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().get("browserHostName").get()
api_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

headers = {"Authorization": f"Bearer {api_token}"}
response = requests.get(f"https://{workspace_url}/api/2.1/jobs/get?job_id={job_id}", headers=headers)

config = response.json()['settings']
with open('/Workspace/.../netflix_workflow_config.json', 'w') as f:
    json.dump(config, f, indent=2)
```

---

## 🎓 For Instructors/Presenters

### Demo Preparation Checklist

- [ ] Ensure `netflix_workflow_config.json` is in your repository
- [ ] Commit `setup_job_workflow.py` to version control
- [ ] Add this README to explain the setup process
- [ ] Test the setup script in a fresh workspace
- [ ] Document any prerequisites (data files, schemas, etc.)

### Quick Demo Flow

1. **Show the repository structure** in GitHub/GitLab
2. **Clone the repo** to Databricks workspace
3. **Run `setup_job_workflow.py`** → Job created in seconds!
4. **Navigate to the job** and click "Run now"
5. **Show the workflow DAG** and task dependencies
6. **Monitor the run** and show Bronze → Silver transformation

---

## 🚀 Advanced: Databricks Asset Bundles (DABs)

For production deployments, consider using **Databricks Asset Bundles**:

* Define infrastructure as code (YAML)
* Support multiple environments (dev/staging/prod)
* Integrated CI/CD deployment
* Git-based version control

**Learn more:** https://docs.databricks.com/dev-tools/bundles/

---

## 📞 Support

If you encounter issues:

1. Check that all notebook paths are correct
2. Verify the `config_table` exists in `workspace.netflix` schema
3. Ensure you have permissions to create jobs
4. Check the setup script output for error messages

---

## 📝 Version History

| Version | Date | Changes |
|---------|------|----------|
| 1.0 | 2026-06-26 | Initial job configuration for Bronze → Silver pipeline |

---

**Happy Data Engineering! 🎬📊**