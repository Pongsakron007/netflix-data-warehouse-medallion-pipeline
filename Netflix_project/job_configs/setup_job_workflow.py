# Databricks notebook source
# MAGIC %md
# MAGIC # 🚀 Netflix Job Workflow Setup Script
# MAGIC 
# MAGIC This script automatically creates the Netflix data pipeline workflow from the stored configuration.
# MAGIC 
# MAGIC **Usage:**
# MAGIC 1. Clone the Netflix project repository
# MAGIC 2. Run this notebook
# MAGIC 3. The job workflow will be created automatically
# MAGIC 
# MAGIC **What it does:**
# MAGIC - Reads the job configuration from `netflix_workflow_config.json`
# MAGIC - Replaces workspace path placeholders with your current user path
# MAGIC - Creates the job using Databricks Jobs API
# MAGIC - Provides the job URL for immediate access

# COMMAND ----------

import json
import requests
import os

# Get current user and workspace information
current_user = spark.sql("SELECT current_user() as user").collect()[0]["user"]
workspace_url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()
api_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

print("=" * 70)
print("NETFLIX JOB WORKFLOW SETUP")
print("=" * 70)
print(f"👤 Current User: {current_user}")
print(f"🌐 Workspace URL: {workspace_url}")
print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load Job Configuration

# COMMAND ----------

# Define paths
project_base_path = f"/Workspace/Users/{current_user}/Databricks-for-Data-Engineers-Bootcamp2/Netflix_project"
config_file_path = f"{project_base_path}/job_configs/netflix_workflow_config.json"

# Read the job configuration
with open(config_file_path, 'r') as f:
    job_config = json.load(f)

print(f"✅ Loaded configuration from: {config_file_path}")
print(f"📋 Job Name: {job_config['name']}")
print(f"📊 Number of Tasks: {len(job_config['tasks'])}")
print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Replace Workspace Path Placeholders

# COMMAND ----------

# Convert config to string, replace placeholder, convert back
config_str = json.dumps(job_config)
config_str = config_str.replace("{{WORKSPACE_PATH}}", project_base_path)
job_config = json.loads(config_str)

print("✅ Updated notebook paths:")
for task in job_config['tasks']:
    print(f"   - {task['task_key']}: {task['notebook_task']['notebook_path']}")
print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Check if Job Already Exists

# COMMAND ----------

# List existing jobs to check if this job name already exists
headers = {
    "Authorization": f"Bearer {api_token}",
    "Content-Type": "application/json"
}

list_jobs_url = f"https://{workspace_url}/api/2.1/jobs/list"
response = requests.get(list_jobs_url, headers=headers)

if response.status_code == 200:
    existing_jobs = response.json().get('jobs', [])
    existing_job = next((job for job in existing_jobs if job['settings']['name'] == job_config['name']), None)
    
    if existing_job:
        print(f"⚠️  Job '{job_config['name']}' already exists (Job ID: {existing_job['job_id']})")
        print(f"🔗 Job URL: https://{workspace_url}/jobs/{existing_job['job_id']}")
        print("\n💡 Options:")
        print("   1. Delete the existing job and re-run this script")
        print("   2. Use the existing job")
        print("   3. Change the job name in the config file")
        dbutils.notebook.exit(f"Job already exists: {existing_job['job_id']}")
    else:
        print(f"✅ Job name '{job_config['name']}' is available")
else:
    print(f"❌ Failed to list jobs: {response.status_code} - {response.text}")
    dbutils.notebook.exit("Failed to check existing jobs")

print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create the Job Workflow

# COMMAND ----------

# Create the job
create_job_url = f"https://{workspace_url}/api/2.1/jobs/create"
payload = job_config

response = requests.post(create_job_url, headers=headers, json=payload)

if response.status_code == 200:
    job_id = response.json()['job_id']
    job_url = f"https://{workspace_url}/jobs/{job_id}"
    
    print("=" * 70)
    print("✅ JOB CREATED SUCCESSFULLY!")
    print("=" * 70)
    print(f"📋 Job Name: {job_config['name']}")
    print(f"🆔 Job ID: {job_id}")
    print(f"🔗 Job URL: {job_url}")
    print()
    print("📊 Workflow Tasks:")
    for i, task in enumerate(job_config['tasks'], 1):
        depends_on = task.get('depends_on', [])
        dependency_str = f" (depends on: {', '.join([d['task_key'] for d in depends_on])})" if depends_on else " (root task)"
        print(f"   {i}. {task['task_key']}{dependency_str}")
    print()
    print("🚀 Next Steps:")
    print(f"   1. Visit the job page: {job_url}")
    print("   2. Click 'Run now' to execute the workflow")
    print("   3. Monitor the pipeline execution in the Runs tab")
    print("=" * 70)
    
    # Return the job ID for programmatic access
    dbutils.notebook.exit(job_id)
else:
    print(f"❌ Failed to create job: {response.status_code}")
    print(f"Response: {response.text}")
    dbutils.notebook.exit("Failed to create job")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📝 Notes for Repository Users
# MAGIC 
# MAGIC **After cloning this project:**
# MAGIC 
# MAGIC 1. **Run this notebook** (`setup_job_workflow.py`) to create the job workflow
# MAGIC 2. **Customize if needed**:
# MAGIC    - Edit `netflix_workflow_config.json` to modify job settings
# MAGIC    - Change cluster settings, schedules, or add more tasks
# MAGIC 3. **Run the workflow**:
# MAGIC    - Go to the job page (URL printed above)
# MAGIC    - Click "Run now" to start the pipeline
# MAGIC 
# MAGIC **To update the job:**
# MAGIC - Delete the existing job from the UI
# MAGIC - Modify `netflix_workflow_config.json`
# MAGIC - Re-run this setup script
# MAGIC 
# MAGIC **For CI/CD deployment:**
# MAGIC - Consider using Databricks Asset Bundles (DABs)
# MAGIC - See: https://docs.databricks.com/dev-tools/bundles/
