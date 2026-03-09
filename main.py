import functions_framework
import datetime
import time
from google.cloud import asset_v1
from google.cloud import bigquery

# --- CONFIGURATION ---
ORG_ID = "<ORG ID>" 
PROJECT_ID = "<PROJECT ID>" 
DATASET_ID = "iam_audit_dataset"
RAW_TABLE = f"{PROJECT_ID}.{DATASET_ID}.raw_iam_data"
FINAL_TABLE = f"{PROJECT_ID}.{DATASET_ID}.final_monthly_report"
BUCKET_NAME = "iam-audit-report" 

@functions_framework.http
def iam_report_to_csv(request):
    start_time = time.time()
    print(f"[{datetime.datetime.now()}] >>> Starting Execution")
    
    client = asset_v1.AssetServiceClient()
    bq_client = bigquery.Client()
    
    try:
        # 1. EXTRACT: Fetching IAM policies
        print(f"[{datetime.datetime.now()}] Step 1: Searching Asset Inventory...")
        scope = f"organizations/{ORG_ID}"
        response = client.search_all_iam_policies(request={"scope": scope})
        
        rows = []
        for result in response:
            # Extract alphanumeric Project ID from the resource URI
            raw_res = result.resource
            p_id = raw_res.split('/')[-1] if "/projects/" in raw_res else "Org-Level"

            for binding in result.policy.bindings:
                role = binding.role
                for member in binding.members:
                    # Filter for Users and Service Accounts
                    if member.startswith(("user:", "serviceAccount:")):
                        rows.append({
                            "role": role, 
                            "member": member.split(":")[-1], 
                            "member_type": "USER" if member.startswith("user:") else "SA",
                            "project_id": p_id
                        })
        
        if not rows:
            print("!!! No relevant records found in Asset Inventory.")
            return "No data found.", 200

        # 2. LOAD RAW DATA (Creates table if missing, Truncates new data if exists)
        print(f"[{datetime.datetime.now()}] Step 2: Loading/Creating Raw Table: {RAW_TABLE}")
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            autodetect=True  # Automatically creates schema on first run
        )
        bq_client.load_table_from_json(rows, RAW_TABLE, job_config=job_config).result()

        # 3. TRANSFORM (Creates table if missing, Replaces if exists)
        print(f"[{datetime.datetime.now()}] Step 3: Generating Final Report Table: {FINAL_TABLE}")
        
        sql_optimized = f"""
            CREATE OR REPLACE TABLE `{FINAL_TABLE}` AS
            SELECT 
                ROW_NUMBER() OVER() as SL_No,
                role as Roles,
                COUNT(DISTINCT member) as Nos,
                STRING_AGG(DISTINCT IF(member_type = 'USER', member, NULL), ', ') as Associated_Users,
                STRING_AGG(DISTINCT IF(member_type = 'SA', member, NULL), ', ') as Associated_Service_Accounts,
                REPLACE(role, 'roles/', '') as Description,
                CASE 
                    WHEN role IN ('roles/owner', 'roles/editor', 'roles/resourcemanager.organizationAdmin') THEN 'Y' 
                    ELSE 'N' 
                END as Privileged_User_YN,
                IF(project_id = 'Org-Level', 'For the entire OU', 'Project Wise') as Remarks,
                project_id as Project_ID
            FROM `{RAW_TABLE}`
            GROUP BY role, project_id
        """
        bq_client.query(sql_optimized).result()

        # 4. EXPORT: GCS Dump
        destination_uri = f"gs://{BUCKET_NAME}/IAM_Report_{datetime.date.today()}.csv"
        print(f"[{datetime.datetime.now()}] Step 4: Exporting report to GCS: {destination_uri}")
        
        # Ensure location matches your dataset location (asia-south1)
        bq_client.extract_table(FINAL_TABLE, destination_uri, location="asia-south1").result()
        
        duration = round(time.time() - start_time, 2)
        print(f"[{datetime.datetime.now()}] >>> Success! Total Execution Time: {duration}s")
        
        return f"Success: Report saved to {destination_uri}", 200

    except Exception as e:
        print(f"[{datetime.datetime.now()}] !!! CRITICAL ERROR: {str(e)}")
        return f"Error: {str(e)}", 500