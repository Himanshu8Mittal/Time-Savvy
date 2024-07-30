"""Performs writing information in database"""

from datetime import date, datetime, timedelta
from tempfile import NamedTemporaryFile
from fastapi import UploadFile
import pandas as pd
from sqlalchemy import extract, and_
from sqlalchemy.orm import Session
from src.db import db_modifier
from src.db.model import models
import src.db.writer_func as wt  # assuming wt contains the checker functions
from sqlalchemy.exc import SQLAlchemyError
import src.db.schema.schemas as schemas
from src.db.model.database import engine


def writing_to_db(df: pd.DataFrame, table, db: Session):
    """Bulk inserts the records into database"""

    records = df.to_dict(orient="records")
    db.bulk_insert_mappings(table, records)  # type: ignore
    db.commit()
    db.close()


def update_to_db(df: pd.DataFrame, table, db: Session):
    """updates/insert employee details in employee data table"""
    df = df.dropna(subset=["indxx_id"])
    records = df.to_dict(orient="records")
    records_to_insert = []
    records_to_update = []
    for record in records:
        existing_record = db.query(table).filter_by(indxx_id=record["indxx_id"]).first()
        if existing_record:
            record["employee_id"] = existing_record.employee_id
            records_to_update.append(record)
        else:
            records_to_insert.append(record)

    if records_to_insert:
        db.bulk_insert_mappings(table, records_to_insert)
    if records_to_update:
        db.bulk_update_mappings(table, records_to_update)

    db.commit()
    db.close()


def save_employee_data_to_db(file: UploadFile, db: Session):
    """CSV to db function"""

    with NamedTemporaryFile(delete=False) as tmp:
        tmp.write(file.file.read())
        tmp.flush()
        df = pd.read_csv(tmp.name)

        df["level_id"] = df.apply(lambda row: wt.level_check(row, db), axis=1)
        df["team_id"] = df.apply(lambda row: wt.team_check(row, db), axis=1)
        df["manager_id"] = df.apply(lambda row: wt.manager_check(row, db), axis=1)
        df["department_id"] = df.apply(lambda row: wt.department_check(row, db), axis=1)
        df["project_code_id"] = df.apply(
            lambda row: wt.project_code_check(row, db), axis=1
        )
        df["project_number_id"] = df.apply(
            lambda row: wt.project_number_check(row, db), axis=1
        )
        df["project_name_id"] = df.apply(
            lambda row: wt.project_name_check(row, db), axis=1
        )

        update_to_db(df, models.EmployeeData, db)


def save_timesheetdata_to_db_streamlit(timesheet_data, db: Session):
    """JSON of timesheet_data to store in database function"""

    try:
        for entry in timesheet_data:

            entry_dict = entry.dict()
            employee = (
                db.query(models.EmployeeData)
                .filter_by(indxx_id=entry_dict["indxx_id"])
                .first()
            )
            employee_id = employee.employee_id

            day_of_month = entry_dict["day_of_month"]
            month = datetime.now().month
            year = datetime.now().year

            existing_entry = (
                db.query(models.TimeSheetData)
                .filter_by(
                    employee_id=employee_id,
                    day_of_month=day_of_month,
                    month=month,
                    year=year,
                )
                .first()
            )

            if existing_entry:
                existing_entry.work_description = entry_dict["work_description"]
                existing_entry.status = entry_dict["status"]

            else:
                new_entry = models.TimeSheetData(
                    employee_id=employee_id,
                    day_of_month=day_of_month,
                    month=month,
                    year=year,
                    work_description=entry_dict["work_description"],
                    status=entry_dict["status"],
                )
                db.add(new_entry)
        db.commit()
    except SQLAlchemyError as e:
        db.rollback()
        print(f"Error: {e}")
    finally:
        db.close()


def delete_entry_from_leavesheet_db(df: pd.DataFrame, table, db: Session):
    """Deletes the records from database"""
    data = df.to_dict(orient="records")

    for record in data:
        db.query(table).filter(
            table.employee_id == record["employee_id"],
            table.leave_status == record["leave_status"],
            table.leave_date == record["leave_date"],
            table.transaction_status == record["transaction_status"],
        ).delete(synchronize_session=False)
    db.commit()
    db.close()


def update_timesheet_adding_new_leavesheet_entries(df: pd.DataFrame, db: Session):
    for index, row in df.iterrows():
        employee_id = row["employee_id"]
        leave_date = row["leave_date"]
        day_of_month = leave_date.day
        month = leave_date.month
        year = leave_date.year

        timesheet_entry = (
            db.query(models.TimeSheetData)
            .filter_by(
                employee_id=employee_id,
                day_of_month=day_of_month,
                month=month,
                year=year,
            )
            .first()
        )

        if timesheet_entry:
            if timesheet_entry.status == "":
                timesheet_entry.status = "Leave"
                timesheet_entry.work_description = ""

    db.commit()
    db.close()


def update_timesheet_removing_old_leavesheet_entries(df: pd.DataFrame, db: Session):
    for index, row in df.iterrows():
        employee_id = row["employee_id"]
        leave_date = row["leave_date"]
        day_of_month = leave_date.day
        month = leave_date.month
        year = leave_date.year

        timesheet_entry = (
            db.query(models.TimeSheetData)
            .filter_by(
                employee_id=employee_id,
                day_of_month=day_of_month,
                month=month,
                year=year,
            )
            .first()
        )

        if timesheet_entry:
            if timesheet_entry.status == "Leave":
                timesheet_entry.status = ""

    db.commit()
    db.close()


def writing_leave_data_to_db(df: pd.DataFrame, db: Session):
    """"""
    year = datetime.now().year
    month = datetime.now().month

    leaves = db.query(models.LeaveSheetData).filter(
        models.LeaveSheetData.leave_date >= datetime(year, month, 1),
        models.LeaveSheetData.leave_date
        <= datetime(year, month, 28) + timedelta(days=3),
        models.LeaveSheetData.leave_status != "Comp Off",
    )

    sql_query = leaves.statement
    previous_df = pd.read_sql(sql_query, engine)

    if previous_df.empty:
        previous_df = pd.DataFrame(
            {
                "leavesheet_id": pd.Series(dtype="int"),
                "employee_id": pd.Series(dtype="int"),
                "leave_status": pd.Series(dtype="str"),
                "leave_date": pd.Series(dtype="str"),
                "transaction_status": pd.Series(dtype="str"),
            }
        )

    previous_df.drop(columns=["leavesheet_id"], inplace=True)
    previous_df["leave_date"] = pd.to_datetime(previous_df["leave_date"])
    df["leave_date"] = pd.to_datetime(df["leave_date"])

    merged_df = pd.merge(
        previous_df,
        df,
        on=["employee_id", "leave_status", "leave_date", "transaction_status"],
        how="outer",
        indicator=True,
    )

    # Filter for entries exclusive to each DataFrame
    previous_df_distinct = merged_df[merged_df["_merge"] == "left_only"].drop(
        columns=["_merge"]
    )
    df_distinct = merged_df[merged_df["_merge"] == "right_only"].drop(
        columns=["_merge"]
    )

    if previous_df_distinct is not None and not previous_df_distinct.empty:
        update_timesheet_removing_old_leavesheet_entries(previous_df_distinct, db)
        delete_entry_from_leavesheet_db(previous_df_distinct, models.LeaveSheetData, db)

    if df_distinct is not None and not df_distinct.empty:
        update_timesheet_adding_new_leavesheet_entries(df_distinct, db)
        writing_to_db(df_distinct, models.LeaveSheetData, db)


def save_leavesheet_data_to_db(file: UploadFile, db: Session):
    """CSV to db function"""

    with NamedTemporaryFile(delete=False) as tmp:

        tmp.write(file.file.read())
        tmp.flush()
        df = pd.read_csv(tmp.name)
        new_header_index = 2
        new_header = df.iloc[new_header_index]

        df = df[3:]
        df.columns = new_header

        df.reset_index(drop=True, inplace=True)

        df = df[df["Leave/Holiday"] != "Work from Home"]
        df = df[df["Number of Days"] != "0.5"]

        df = df.drop(
            [
                "Name of the Employee",
                "Number of Days",
                "Remarks",
                "Applied On",
                "Contact Details",
            ],
            axis=1,
        )

        df["From Date"] = pd.to_datetime(df["From Date"], format="%d-%b-%y")
        df["To Date"] = pd.to_datetime(df["To Date"], format="%d-%b-%y")

        result = []
        current_year = datetime.now().year
        current_month = datetime.now().month

        for idx, row in df.iterrows():
            date_range = pd.date_range(start=row["From Date"], end=row["To Date"])

            for date in date_range:
                if date.year == current_year and date.month == current_month:
                    result.append(
                        {
                            "indxx_id": row["Employee No"],
                            "leave_status": row["Leave/Holiday"],
                            "leave_date": date,
                            "transaction_status": row["Transaction Status"],
                        }
                    )

        result_df = pd.DataFrame(result)

        if result_df is not None and not result_df.empty:
            result_df.columns = [
                "indxx_id",
                "leave_status",
                "leave_date",
                "transaction_status",
            ]

            result_df["employee_id"] = result_df.apply(
                lambda row: wt.indxx_id_check(row, db), axis=1
            )
            result_df = result_df.drop("indxx_id", axis=1)
            result_df = result_df.iloc[:, [3, 0, 1, 2]]
            writing_leave_data_to_db(result_df, db)


def writing_comp_off_data_to_database(df: pd.DataFrame, db: Session):
    """"""

    records = df.to_dict(orient="records")
    records_to_insert = []
    records_to_update = []
    for record in records:
        existing_record = (
            db.query(models.LeaveSheetData)
            .filter_by(
                employee_id=record["employee_id"],
                leave_status=record["leave_status"],
                leave_date=record["leave_date"],
            )
            .first()
        )

        if existing_record:
            # record["transaction_status"] = existing_record.transaction_status
            record["leavesheet_id"] = existing_record.leavesheet_id
            records_to_update.append(record)
        else:
            records_to_insert.append(record)

    if records_to_insert:
        db.bulk_insert_mappings(models.LeaveSheetData, records_to_insert)
    if records_to_update:
        db.bulk_update_mappings(models.LeaveSheetData, records_to_update)

    db.commit()
    db.close()


def create_comp_off_df(
    db: Session, indxx_id: str, from_date: date, to_date: date, transaction_status: str
):
    """Comp Off data is added to database"""
    employee = db.query(models.EmployeeData).filter_by(indxx_id=indxx_id).first()
    if employee is not None:
        employee_id = employee.employee_id
    else:
        raise ValueError("Indxx ID does not exist in the database")

    result = []
    current_year = datetime.now().year
    current_month = datetime.now().month

    date_range = pd.date_range(start=from_date, end=to_date)

    for date in date_range:
        if date.year == current_year and date.month == current_month:
            result.append(
                {
                    "employee_id": employee_id,
                    "leave_status": "Comp Off",
                    "leave_date": date,
                    "transaction_status": transaction_status,
                }
            )

    result_df = pd.DataFrame(result)
    writing_comp_off_data_to_database(result_df, db)


def write_holiday_to_db(df: pd.DataFrame, model, db: Session):
    """This function helps to insert or update the data of holidays in the database"""
 
    df['holiday_date'] = df['holiday_date'].apply(lambda x:x.date())
 
    records_to_update = []
    records_to_delete = []
    for _, row in df.iterrows():
        if (row["holiday_date"].month >= date.today().month) and (row["holiday_date"].year >= date.today().year):
            existing_record = (
                db.query(model).filter(model.holiday == row["holiday"]).first()
            )
            if existing_record:
                if existing_record.holiday_date != row["holiday_date"]:
                    records_to_delete.append(existing_record.holiday_date)
                    records_to_update.append(row["holiday_date"])
                    existing_record.holiday_date = row["holiday_date"]
 
            else:
                records_to_update.append(row["holiday_date"])
                new_record = model(holiday_date=row["holiday_date"], holiday=row["holiday"])
                db.add(new_record)
    db.commit()
 
 
    for entry in records_to_update:
        db.query(models.TimeSheetData).filter(
            models.TimeSheetData.day_of_month == entry.day,
            models.TimeSheetData.month == entry.month,# .date()
            models.TimeSheetData.year == entry.year,
            models.TimeSheetData.status != "Saturday",
            models.TimeSheetData.status != "Sunday",
        ).update(
            {
                models.TimeSheetData.work_description: '',
                models.TimeSheetData.status: 'Holiday'
            }
        )
 
    for entry in records_to_delete:
        db.query(models.TimeSheetData).filter(
                models.TimeSheetData.day_of_month == entry.day,
                models.TimeSheetData.month == entry.month,
                models.TimeSheetData.year == entry.year,
                models.TimeSheetData.status == "Holiday",
            ).update(
                {
                    models.TimeSheetData.status: ''
                }
            )
       
    db.commit()
    db.close()
    

def save_holiday_data_to_db(uploaded_file: UploadFile, db: Session):
    """CSV to db function"""

    with NamedTemporaryFile(delete=False) as tmp:

        tmp.write(uploaded_file.file.read())
        tmp.flush()
        df = pd.read_csv(tmp.name)

        df["holiday_date"] = pd.to_datetime(df["holiday_date"], format="%d-%m-%Y")

        df.columns = [
            "holiday_date",
            "holiday",
        ]

        write_holiday_to_db(df, models.HolidayData, db)


def create_user_role(db: Session, user: schemas.RoleCreate):
    """Creates and updates user roles"""
    empid = db.query(models.EmployeeData.employee_id).filter_by(indxx_id=user.indxx_id).first()
    if empid:
        employee_id = empid.employee_id
    else:
       raise ValueError("User with given Index ID does not exist.")
    check = db.query(models.RoleData).filter_by(employee_id=employee_id).first()
    if check:
        if (user.is_super_user == False) and (user.is_admin == False):
            db.delete(check)
            db.commit()
            db.close()
            return {"message": f"{user.indxx_id} is now a Normal User"}
   
        elif (check.is_admin == user.is_admin) and (check.is_super_user == user.is_super_user):
            return {"message": "Role already allocated"}
            
        else:
            check.is_admin = user.is_admin
            check.is_super_user = user.is_super_user
            db.commit()
            db.close()
            if(user.is_super_user):
                return {"message": f"{user.indxx_id} is now a Super User"}
            else:
                return {"message": f"{user.indxx_id} is now an Admin"}

    else:
        if user.is_super_user == False and user.is_admin == False:
            return {"message": f"{user.indxx_id} is now a Normal User"}
        
        else:
            new_entry = models.RoleData(
                employee_id=employee_id,
                is_super_user=user.is_super_user,
                is_admin=user.is_admin,
            )
            db.add(new_entry)
            db.commit()
            db.close()
            if(user.is_super_user):
                return {"message": f"{user.indxx_id} is now a Super User"}
            else:
                return {"message": f"{user.indxx_id} is now an Admin"}

def insert_update_in_timewindow(window_data: schemas.TimeWindow, db: Session) -> None:
    """Function to insert update timesheet window status"""
    super_user_id = (
        db.query(models.EmployeeData)
        .filter_by(indxx_id=window_data.super_user_id)
        .first()
    )
    if not super_user_id:
        raise ValueError("Super User does not exist!!")
    if window_data.freeze is True and window_data.unfreeze is False:
        new_entry = models.TimeWindowData(
            super_user_id=super_user_id.employee_id,
            status="Freeze",
        )
    elif window_data.freeze is False and window_data.unfreeze is True:
        new_entry = models.TimeWindowData(
            super_user_id=super_user_id.employee_id,
            status="Unfreeze",
        )
    db.add(new_entry)
    db.commit()
    db.close()
