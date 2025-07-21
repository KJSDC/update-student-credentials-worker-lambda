import pymongo
import logging
import re
from datetime import datetime

from pymongo import UpdateOne

from constants import *

logger = logging.getLogger()
logger.setLevel(logging.INFO)

if not logger.hasHandlers():
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(levelname)s:%(name)s:%(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

mongo_client = None
DATABASE = None

def initialize_mongo_client() -> None:
    global mongo_client, DATABASE
    if mongo_client is None:
        try:
            logger.info("Initializing MongoDB client...")
            mongo_client = pymongo.MongoClient(
                MONGO_CONNECTION_URI,
                maxPoolSize=5,
                connectTimeoutMS=3000,
                serverSelectionTimeoutMS=5000,
                retryWrites=True
            )
            DATABASE = mongo_client.get_database(MONGO_DATABASE)
            logger.info("MongoDB connection initialized")
        except Exception as e:
            logger.exception("Failed to initialize MongoDB client")
            raise

def map_excel_row_to_db_fields(row: dict) -> dict:
    """
    Convert a row from Excel headers to collection field names.
    Handles:
    - Status: string 'active'/'inactive' to boolean.
    - *_Text: to uppercase.
    - *_Date: formats 'dd-mm-yyyy' or 'dd/mm/yyyy' to millis.
    Ignores unmapped fields.
    """
    mapped = {}
    for excel_col, value in row.items():
        db_field = EXCEL_TO_DB_FIELD_MAP.get(excel_col)
        if not db_field:
            continue

        # Status field conversion
        if db_field == "isActive_KJUSYSCommon_Bool":
            if isinstance(value, str) and value.strip().lower() == "active":
                mapped[db_field] = True
            elif isinstance(value, str) and value.strip().lower() == "inactive":
                mapped[db_field] = False
            else:
                mapped[db_field] = False  # or skip

        # _Text fields to uppercase
        elif db_field.endswith("_Text"):
            if isinstance(value, str):
                mapped[db_field] = value.upper()
            elif value is not None:
                mapped[db_field] = str(value).upper()
            else:
                mapped[db_field] = ""

        # _Date fields to millis
        elif db_field.endswith("_Date"):
            millis = None
            if isinstance(value, str):
                # Acceptable formats: dd-mm-yyyy or dd/mm/yyyy
                match = re.match(r"(\d{2})[-/](\d{2})[-/](\d{4})", value.strip())
                if match:
                    day, month, year = match.groups()
                    try:
                        dt = datetime(int(year), int(month), int(day))
                        millis = int(dt.timestamp() * 1000)
                    except Exception:
                        millis = None
            mapped[db_field] = millis

        # Default: keep as-is
        else:
            mapped[db_field] = value
    return mapped

def lambda_handler(event, context):
    """
    Receives a batch of rows (list of dicts), each with Excel headers.
    Updates student collection documents by application number.
    """
    logger.info("Worker lambda: starting batch update")

    failed_application_numbers = []

    try:
        initialize_mongo_client()
        student_profile_collection = DATABASE.get_collection(ERP_STUDENT_PROFILE_COLLECTION)

        batch = event.get("batch", [])
        if not batch or not isinstance(batch, list):
            return {
                "success": False,
                "message": "No batch data provided or batch is not a list",
                "failedRows": []
            }

        operations = []
        app_numbers_in_batch = []

        for row in batch:
            mapped_fields = map_excel_row_to_db_fields(row)
            app_no = mapped_fields.get("applicationNumber_ErpStudentProfile_Text")
            if not app_no:
                failed_application_numbers.append(row.get("Application Number", ""))
                continue
            app_numbers_in_batch.append(app_no)
            update_fields = {k: v for k, v in mapped_fields.items() if k != "applicationNumber_ErpStudentProfile_Text"}
            if not update_fields:
                continue  # nothing to update
            operations.append(
                UpdateOne(
                    {"applicationNumber_ErpStudentProfile_Text": app_no},
                    {"$set": update_fields}
                )
            )

        if operations:
            result = student_profile_collection.bulk_write(operations, ordered=False)
            modified_count = result.modified_count
            logger.info(f"Updated {modified_count} record(s)")
            
            # Find which app numbers were not modified (possibly not present in DB)
            updated_app_numbers = set()
            # There is no direct way to get which documents were not found in bulk_write,
            # so fetch which application numbers are present after update.
            present_docs = student_profile_collection.find(
                {"applicationNumber_ErpStudentProfile_Text": {"$in": app_numbers_in_batch}},
                {"applicationNumber_ErpStudentProfile_Text": 1, "_id": 0}
            )
            updated_app_numbers = {doc["applicationNumber_ErpStudentProfile_Text"] for doc in present_docs}
            failed_application_numbers += [app_no for app_no in app_numbers_in_batch if app_no not in updated_app_numbers]

        else:
            logger.info("No updates to perform (no mappable fields found in batch)")

        success = len(failed_application_numbers) == 0
        return {
            "success": success,
            "message": "All rows updated" if success else "Some records failed",
            "failedRows": failed_application_numbers
        }

    except Exception as e:
        logger.exception("Unexpected exception in worker lambda")
        return {
            "success": False,
            "message": f"Unhandled exception: {str(e)}",
            "failedRows": failed_application_numbers + [row.get("Application Number", "") for row in batch]
        }