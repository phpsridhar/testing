# ===================================================================
#                SPARK RECONCILIATION ENGINE (FULL)
# ===================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders

# -------------------------------------------------------------
# CONFIG SECTION
# -------------------------------------------------------------

schema1 = "SCHEMA_A"
schema2 = "SCHEMA_B"

table_csv = "/path/to/table_list.csv"          # Input table list
output_root = "/workspace/recon_output/"       # Spark output
final_csv_folder = "/workspace/final_csv/"     # Final merged CSVs

oracle_url = "jdbc:oracle:thin:@host:port/service"
user = "username"
password = "password"

exclude_cols = ["UPDATED_TS", "LOAD_TS"]

# PK mapping (extend if needed)
pk_mapping = {
    "CUSTOMER": ["CUST_ID"],
    "ACCOUNTS": ["ACC_ID"],
    "TRANSACTION": ["TXN_ID"],
    "LOANS": ["LOAN_ID"]
}

# Email settings
smtp_server = "smtp.yourcompany.com"
smtp_port = 587
smtp_user = "your.email@company.com"
smtp_password = "password"
recipients = ["team@company.com"]

# -------------------------------------------------------------
# START SPARK
# -------------------------------------------------------------

spark = SparkSession.builder.appName("Full_Reconciliation_Engine").getOrCreate()

# Read table list
tables_df = spark.read.option("header", "true").csv(table_csv)
table_list = [row["TABLE_NAME"] for row in tables_df.collect()]

# -------------------------------------------------------------
# HELPER FUNCTIONS
# -------------------------------------------------------------

def load_table(schema, table):
    return (
        spark.read.format("jdbc")
        .option("url", oracle_url)
        .option("user", user)
        .option("password", password)
        .option("dbtable", f"{schema}.{table}")
        .load()
    )

def merge_part_files(src_folder, final_file):
    os.makedirs(os.path.dirname(final_file), exist_ok=True)
    first_write = True
    with open(final_file, "w") as fw:
        for f in sorted(os.listdir(src_folder)):
            if f.startswith("part-"):
                with open(os.path.join(src_folder, f)) as fr:
                    if first_write:
                        fw.write(fr.read())
                        first_write = False
                    else:
                        next(fr)  # skip headers
                        fw.write(fr.read())

# -------------------------------------------------------------
# MAIN LOOP OVER TABLES
# -------------------------------------------------------------

summary_records = []   # For summary.csv

for table in table_list:
    print(f"\n\n===== Processing Table: {table} =====")

    pk = pk_mapping[table]

    df_a = load_table(schema1, table)
    df_b = load_table(schema2, table)

    cols_a = set(df_a.columns)
    cols_b = set(df_b.columns)

    compare_cols = sorted(list((cols_a & cols_b) - set(pk) - set(exclude_cols)))

    df_a = df_a.select(*(pk + compare_cols))
    df_b = df_b.select(*(pk + compare_cols))

    joined = df_a.alias("A").join(df_b.alias("B"), pk, "inner")

    mismatch_df = joined.filter(
        sum([(col(f"A.{c}") != col(f"B.{c}")).cast("int") for c in compare_cols]) > 0
    )

    # Side-by-side mismatch report
    side_cols = []
    for c in compare_cols:
        side_cols.append(col(f"A.{c}").alias(f"{c}_A"))
        side_cols.append(col(f"B.{c}").alias(f"{c}_B"))

    report_df = mismatch_df.select(*( [col(f"A.{c}") for c in pk] + side_cols ))

    # Count mismatches
    mismatch_count = report_df.count()
    summary_records.append((table, mismatch_count))

    # Write per-table spark CSV
    spark_out = f"{output_root}/{table}/"
    report_df.write.mode("overwrite").option("header", "true").csv(spark_out)

# -------------------------------------------------------------
# CREATE FINAL CLEAN CSV FILES
# -------------------------------------------------------------

os.makedirs(final_csv_folder, exist_ok=True)

for table in table_list:
    merged_file = os.path.join(final_csv_folder, f"{table}_mismatch.csv")
    part_folder = os.path.join(output_root, table)
    merge_part_files(part_folder, merged_file)
    print(f"Merged CSV created: {merged_file}")

# -------------------------------------------------------------
# CREATE SUMMARY CSV (NO pandas)
# -------------------------------------------------------------

summary_file = os.path.join(final_csv_folder, "summary.csv")
with open(summary_file, "w") as fw:
    fw.write("TABLE_NAME,MISMATCH_COUNT\n")
    for tbl, cnt in summary_records:
        fw.write(f"{tbl},{cnt}\n")

print("Summary CSV created:", summary_file)

# -------------------------------------------------------------
# EMAIL ALL CSV FILES
# -------------------------------------------------------------

msg = MIMEMultipart()
msg["From"] = smtp_user
msg["To"] = ", ".join(recipients)
msg["Subject"] = "Daily Reconciliation Report – Schema Comparison"

body = """Hi Team,

Please find attached the reconciliation reports for all tables.

Regards,
Sridharan
"""

msg.attach(MIMEText(body, "plain"))

# Attach all CSV files
for file in os.listdir(final_csv_folder):
    if file.endswith(".csv"):
        file_path = os.path.join(final_csv_folder, file)

        part = MIMEBase("application", "octet-stream")
        with open(file_path, "rb") as f:
            part.set_payload(f.read())
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", f"attachment; filename={file}")

        msg.attach(part)

server = smtplib.SMTP(smtp_server, smtp_port)
server.starttls()
server.login(smtp_user, smtp_password)
server.sendmail(smtp_user, recipients, msg.as_string())
server.quit()

print("\n\nMail sent successfully!")
print("Reconciliation job completed successfully.")
