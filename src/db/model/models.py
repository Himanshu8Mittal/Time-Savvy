"""Defining the models for the database"""

import datetime

from sqlalchemy import Boolean, Column, Date, DateTime, ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import func
from .database import Base


class RoleData(Base):
    """Defining the model for roles allocation"""

    __tablename__ = "role_data"
    serial_id = Column(Integer, primary_key=True, autoincrement=True)
    employee_id = Column(
        Integer, ForeignKey("employee_data.employee_id"), unique=True, nullable=False
    )
    is_super_user = Column(Boolean, default=False)
    is_admin = Column(Boolean, default=False)
    employees = relationship("EmployeeData", back_populates="role")


class LevelData(Base):
    """Defining the model for level_data table"""

    __tablename__ = "level_data"
    level_id = Column(Integer, primary_key=True, autoincrement=True)
    level = Column(String, unique=True, nullable=False)
    employees = relationship("EmployeeData", back_populates="level")


class TeamData(Base):
    """Defining the model for team_data table"""

    __tablename__ = "team_data"
    team_id = Column(Integer, primary_key=True, autoincrement=True)
    team = Column(String, unique=True, nullable=False)
    employees = relationship("EmployeeData", back_populates="team")


class DepartmentData(Base):
    """Defining the model for department_data table"""

    __tablename__ = "department_data"
    department_id = Column(Integer, primary_key=True, autoincrement=True)
    department = Column(String, unique=True, nullable=False)
    employees = relationship("EmployeeData", back_populates="department")


class ManagerData(Base):
    """Defining the model for manager_data table"""

    __tablename__ = "manager_data"
    manager_id = Column(Integer, primary_key=True, autoincrement=True)
    manager = Column(String, nullable=False)
    employees = relationship("EmployeeData", back_populates="manager")


class ProjectNumberData(Base):
    """Defining the model for project_number_data table"""

    __tablename__ = "project_number_data"
    project_number_id = Column(Integer, primary_key=True, autoincrement=True)
    project_number = Column(String, unique=True, nullable=False)
    employees = relationship("EmployeeData", back_populates="project_number")


class ProjectCodeData(Base):
    """Defining the model for project_code_data table"""

    __tablename__ = "project_code_data"
    project_code_id = Column(Integer, primary_key=True, autoincrement=True)
    project_code = Column(String, unique=True, nullable=False)
    employees = relationship("EmployeeData", back_populates="project_code")


class ProjectNameData(Base):
    """Defining the model for project_name_data table"""

    __tablename__ = "project_name_data"
    project_name_id = Column(Integer, primary_key=True, autoincrement=True)
    project_name = Column(String, unique=True, nullable=False)
    employees = relationship("EmployeeData", back_populates="project_name")


class EmployeeData(Base):
    """Defining the model for employee_data table"""

    __tablename__ = "employee_data"
    employee_id = Column(Integer, primary_key=True, auto_increment=True)
    indxx_id = Column(String(10), unique=True)
    hr_code = Column(String(10), unique=True)
    first_name = Column(String(50), nullable=False)
    last_name = Column(String(100))
    start_date = Column(Date)
    level_id = Column(Integer, ForeignKey("level_data.level_id"))
    team_id = Column(Integer, ForeignKey("team_data.team_id"))
    department_id = Column(Integer, ForeignKey("department_data.department_id"))
    manager_id = Column(Integer, ForeignKey("manager_data.manager_id"))
    project_number_id = Column(
        Integer, ForeignKey("project_number_data.project_number_id")
    )
    project_code_id = Column(Integer, ForeignKey("project_code_data.project_code_id"))
    project_name_id = Column(Integer, ForeignKey("project_name_data.project_name_id"))
    level = relationship("LevelData", back_populates="employees")
    team = relationship("TeamData", back_populates="employees")
    department = relationship("DepartmentData", back_populates="employees")
    manager = relationship("ManagerData", back_populates="employees")
    project_number = relationship("ProjectNumberData", back_populates="employees")
    project_code = relationship("ProjectCodeData", back_populates="employees")
    project_name = relationship("ProjectNameData", back_populates="employees")
    time_sheet = relationship("TimeSheetData", back_populates="employees")
    leave_sheet = relationship("LeaveSheetData", back_populates="employees")
    role = relationship("RoleData", back_populates="employees", uselist=False)


class TimeSheetData(Base):
    """Defining timesheet models"""

    __tablename__ = "timesheet_data"
    timesheet_id = Column(Integer, primary_key=True, autoincrement=True)
    employee_id = Column(Integer, ForeignKey("employee_data.employee_id"))
    day_of_month = Column(Integer)
    month = Column(Integer)
    year = Column(Integer)
    work_description = Column(String(100))
    status = Column(String(20))

    employees = relationship("EmployeeData", back_populates="time_sheet")


class LeaveSheetData(Base):
    """Defining leavesheet models"""

    __tablename__ = "leavesheet_data"
    leavesheet_id = Column(Integer, primary_key=True, autoincrement=True)
    employee_id = Column(Integer, ForeignKey("employee_data.employee_id"))
    leave_status = Column(String(100))
    leave_date = Column(Date)
    transaction_status = Column(String(20))

    employees = relationship("EmployeeData", back_populates="leave_sheet")


class HolidayData(Base):
    """Defining holiday data models"""

    __tablename__ = "holiday_data"
    holiday_id = Column(Integer, primary_key=True, autoincrement=True)
    holiday_date = Column(Date, nullable=False)
    holiday = Column(String(100))


class TimeWindowData(Base):
    """Defining time window model"""

    __tablename__ = "time_window_data"
    window_id = Column(Integer, primary_key=True, autoincrement=True)
    time_stamp: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    super_user_id = Column(Integer, ForeignKey("role_data.employee_id"),nullable=False)
    status = Column(String(50))
