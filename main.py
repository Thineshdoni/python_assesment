import os
import sys
import logging
from pathlib import Path
import subprocess
from datetime import datetime
import boto3

# ---------------- Logging ----------------
logging.basicConfig(
    level=logging.INFO,
    filename='app.log',
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logging.info("log configured")
# ---------------- Environment Variables ----------------
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = os.getenv("DB_PORT", "3306")
BACKUP_DIR = os.getenv("BACKUP_DIR", ".")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
# Validate envs
required_vars = [DB_USER, DB_PASSWORD, DB_NAME]
if not all(required_vars):
    logging.error("Missing required database environment variables")
    sys.exit(1)

# ---------------- Backup Path ----------------
backup_name = f"{DB_NAME}-{datetime.now().strftime('%Y%m%d-%H%M%S')}.sql.gz"
backup_path = Path(BACKUP_DIR) / backup_name
backup_path.parent.mkdir(parents=True, exist_ok=True)

# ---------------- Command ----------------
cmd = [
    "mysqldump",
    "-u", DB_USER,
    "-h", DB_HOST,
    "-P", DB_PORT,
    f"-p{DB_PASSWORD}",
    DB_NAME
]
logging.info("Backup started")
# ---------------- Backup Function ----------------
def create_mydb_backup():
    try:
        with open(backup_path, "wb") as backup_file:
            dump = subprocess.Popen(cmd, stdout=subprocess.PIPE)
            gzip = subprocess.Popen(["gzip"], stdin=dump.stdout, stdout=backup_file)

            dump.stdout.close()
            gzip.communicate()

        logging.info(f"Backup completed: {backup_path}")

    except subprocess.CalledProcessError as e:
        logging.error(f"mysqldump failed: {e}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Backup failed: {e}")
        sys.exit(1)




def s3_upload():
    client = boto3.client('s3')
    try:
        client.upload_file(str(backup_path), S3_BUCKET_NAME, backup_name)
        logging.info(f"Backup uploaded to S3: s3://{S3_BUCKET_NAME}/{backup_name}")
    except Exception as e:
        logging.error(f"Backup S3 upload failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    create_mydb_backup()
    s3_upload()
