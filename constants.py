import os

# COLLECTION NAMES
ERP_STUDENT_PROFILE_COLLECTION = "erp_student_profile"
AUTH_USERS_COLLECTION = "auth_users"
AUTH_ROLES_COLLECTION = "auth_roles"

# CONFIG
MONGO_WRITE_BATCH_SIZE = 500

# ENV VARIABLES
MONGO_CONNECTION_URI = os.environ.get('MONGO_CONNECTION_URI')
MONGO_DATABASE = os.environ.get('MONGO_DATABASE')

# Map Excel column headers to collection field names
EXCEL_TO_DB_FIELD_MAP = {
    "Application Number": "applicationNumber_ErpStudentProfile_Text",
    "RollNo": "studentRollNumber_ErpStudentProfile_Text",
    "Semester": "studentSemester_ErpStudentProfile_Int",
    "Semester Type": "studentSemesterType_ErpStudentProfile_Text",
    "Class": "studentClass_ErpStudentProfile_Text",
    "College Email Id": "studentCollegeEmail_ErpStudentProfile_Text",
    "Date Of Admission": "studentDateOfAdmission_ErpStudentProfile_Date",
    "Status": "isActive_KJUSYSCommon_Bool"
}
