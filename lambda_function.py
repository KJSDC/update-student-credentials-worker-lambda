import pymongo
import logging
import re
from datetime import datetime, timezone
import bcrypt
from bson import ObjectId
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
                mapped[db_field] = False

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
                        dt = datetime(int(year), int(month), int(day), tzinfo=timezone.utc)
                        millis = int(dt.timestamp() * 1000)
                    except Exception:
                        millis = None
            mapped[db_field] = millis

        # _Int fields to int
        elif db_field.endswith("_Int"):
            try:
                mapped[db_field] = int(value)
            except:
                mapped[db_field] = None

        # Default: keep as-is
        else:
            mapped[db_field] = value
    return mapped

def hash_bcrypt(input_str: str) -> str:
    if not input_str:
        raise ValueError("Input string for hashing cannot be empty")
    salt = bcrypt.gensalt(rounds=10)
    hashed = bcrypt.hashpw(input_str.encode("utf-8"), salt)
    return hashed.decode("utf-8")

def get_student_auth_role_object_id():
    """
    Fetch the ObjectId for the STUDENT role from the auth roles collection.
    """
    auth_roles_collection = DATABASE.get_collection(AUTH_ROLES_COLLECTION)
    doc = auth_roles_collection.find_one({"authRoleName_AuthCommon_Text": "STUDENT"}, {"_id": 1})
    if doc and "_id" in doc:
        return doc["_id"]
    else:
        logger.error("STUDENT auth role not found in auth roles collection.")
        return None

def lambda_handler(event, context):
    """
    Receives a batch of rows (list of dicts), each with Excel headers.
    Updates student collection documents by application number.
    """
    logger.info("Worker lambda: starting batch update")

    failed_application_numbers = []
    batch = event.get("batch", []) if isinstance(event, dict) else []

    try:
        initialize_mongo_client()
        student_profile_collection = DATABASE.get_collection(ERP_STUDENT_PROFILE_COLLECTION)
        auth_users_collection = DATABASE.get_collection(AUTH_USERS_COLLECTION)
        student_role_id = get_student_auth_role_object_id()

        if not batch or not isinstance(batch, list):
            return {
                "success": False,
                "message": "No batch data provided or batch is not a list",
                "failedRows": []
            }

        operations = []
        credentials_operations = []
        app_numbers_in_batch = []

        for row in batch:
            mapped_fields = map_excel_row_to_db_fields(row)
            app_no = mapped_fields.get("applicationNumber_ErpStudentProfile_Text")
            college_email = mapped_fields.get("studentCollegeEmail_ErpStudentProfile_Text")

            if not app_no:
                failed_application_numbers.append(row.get("Application Number", ""))
                continue

            app_numbers_in_batch.append(app_no)
            update_fields = {k: v for k, v in mapped_fields.items() if k != "applicationNumber_ErpStudentProfile_Text"}
            if update_fields:
                operations.append(
                    UpdateOne(
                        {"applicationNumber_ErpStudentProfile_Text": app_no},
                        {"$set": update_fields}
                    )
                )
            
            # Add credentials for user
            if college_email:
                now_millis = int(datetime.now(timezone.utc).timestamp() * 1000)
                try:
                    hashed_password = hash_bcrypt(college_email)
                except Exception as e:
                    logger.error(f"Failed to hash password for email {college_email}: {e}")
                    failed_application_numbers.append(app_no)
                    continue
                # Use the same status as in student profile
                is_active_status = mapped_fields.get("isActive_KJUSYSCommon_Bool", True)
                credentials_doc = {
                    "userEmail_AuthCommon_Text": college_email,
                    "userPassword_AuthCommon_Text": hashed_password,
                    "isActive_KJUSYSCommon_Bool": is_active_status,
                    "createdOn_KJUSYSCommon_DateTime": now_millis,
                    "authRoles_AuthCommon_ObjectIdArray": [
                        student_role_id if isinstance(student_role_id, ObjectId) else ObjectId(student_role_id)
                    ] if student_role_id else []
                }
                credentials_operations.append(
                    UpdateOne(
                        {"userEmail_AuthCommon_Text": college_email},
                        {"$set": credentials_doc},
                        upsert=True
                    )
                )
        # Execute student updates
        if operations:
            result = student_profile_collection.bulk_write(operations, ordered=False)
            modified_count = result.modified_count
            logger.info(f"Updated {modified_count} record(s)")
            
            # Find which app numbers were not modified (possibly not present in DB)
            present_docs = student_profile_collection.find(
                {"applicationNumber_ErpStudentProfile_Text": {"$in": app_numbers_in_batch}},
                {"applicationNumber_ErpStudentProfile_Text": 1, "_id": 0}
            )
            updated_app_numbers = {doc["applicationNumber_ErpStudentProfile_Text"] for doc in present_docs}
            failed_application_numbers += [app_no for app_no in app_numbers_in_batch if app_no not in updated_app_numbers]

        else:
            logger.info("No updates to perform (no mappable fields found in batch)")
        
        # Execute credentials upserts
        if credentials_operations:
            credentials_result = auth_users_collection.bulk_write(credentials_operations, ordered=False)
            logger.info(f"Credentials upserted: {credentials_result.upserted_count}, modified: {credentials_result.modified_count}")

        success = len(failed_application_numbers) == 0
        return {
            "success": success,
            "message": "All rows updated" if success else "Some records failed",
            "failedRows": failed_application_numbers
        }

    except Exception as e:
        logger.exception("Unexpected exception in worker lambda")
        # batch is always defined above
        return {
            "success": False,
            "message": f"Unhandled exception: {str(e)}",
            "failedRows": failed_application_numbers + [
                row.get("Application Number", "") for row in batch
                if row.get("Application Number", "") not in failed_application_numbers
            ]
        }