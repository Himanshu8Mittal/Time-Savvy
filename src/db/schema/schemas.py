"""Schemas of Response Model"""

from datetime import date, time
from typing import List
from typing import Optional
from pydantic import BaseModel, Field


class ProjectNameData(BaseModel):
    """Class of Project Name details"""

    project_name_id: int
    project_name: str

    class Config:
        """Specifying the ORM mode"""

        orm_mode = True


class ProjectCodeData(BaseModel):
    """Class of Project Code details"""

    project_code_id: int
    project_code: str

    class Config:
        """Specifying the ORM mode"""

        orm_mode = True


class ProjectNumberData(BaseModel):
    """Class of Project Number details"""

    project_number_id: int
    project_number: str

    class Config:
        """Specifying the ORM mode"""

        orm_mode = True


class TeamData(BaseModel):
    """Class of Team details"""

    team_id: int
    team: str

    class Config:
        """Specifying the ORM mode"""

        orm_mode = True


class LevelData(BaseModel):
    """Class of Level details"""

    level_id: int
    level: str

    class Config:
        """Specifying the ORM mode"""

        orm_mode = True


class ManagerData(BaseModel):
    """Class of Manager details"""

    manager_id: int
    manager: str

    class Config:
        """Specifying the ORM mode"""

        orm_mode = True


class DepartmentData(BaseModel):
    """Class of Department details"""

    department_id: int
    department: str

    class Config:
        """Specifying the ORM mode"""

        orm_mode = True


class RoleData(BaseModel):
    """Schema for role allocation"""

    is_super_user: bool = Field(default=False)
    is_admin: bool = Field(default=False)

    class Config:
        """Specifying the ORM mode"""

        orm_mode = True


class RoleCreate(RoleData):
    """Schema for creation of role"""

    indxx_id: str


class EmployeeData(BaseModel):
    """Class of overall details of an employee"""

    indxx_id: str
    hr_code: str
    first_name: str
    last_name: str
    start_date: date
    level_id: int
    department_id: int
    manager_id: int
    project_number_id: int
    project_code_id: int
    project_name_id: int
    level: LevelData
    team: TeamData
    department: DepartmentData
    manager: ManagerData
    project_number: ProjectNumberData
    project_code: ProjectCodeData
    project_name: ProjectNameData
    role: Optional[RoleData]

    class Config:
        """Specifying the ORM mode"""

        orm_mode = True


class TimeSheetData(BaseModel):
    """Schema for time sheet storage"""

    day_of_month: int
    work_description: str
    status: str
    IN: str
    OUT: str
    indxx_id: str


class LeaveData(BaseModel):
    """Schema for leave data storage"""

    day_of_month: int
    status: str


class HolidayData(BaseModel):
    """Schema for holiday data"""

    holiday_date: date
    holiday: str


class StoxxSheet(BaseModel):
    """Schema for stoxx sheet generation"""

    project_code: List
    month: int
    year: int


class CompOffData(BaseModel):
    """Schema for comp off data"""

    indxx_id: str
    from_date: date
    to_date: date
    transaction_status: str


class SelectedOptions(BaseModel):
    """Schema for selected options of project names"""

    project_names_list: List[str]


class TimeWindow(BaseModel):
    """Schema for freeze unfreeze options"""

    freeze: bool = Field(default=False)
    unfreeze: bool = Field(default=False)
    super_user_id: str