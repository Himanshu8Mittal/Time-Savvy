import os
import sys

sys.path.insert(0, os.getcwd())
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import StaticPool, create_engine
from sqlalchemy.orm import sessionmaker
from src.db.model.database import Base, get_db
from src.db.model.models import *
from src.main import app
from datetime import date

DATABASE_URL = "sqlite:///./test_db.db"

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
)

TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def override_get_db():
    """Provides a test-specific database session.
    Creates and yields a new database session using the `TestingSessionLocal` configuration for testing purposes.
    Ensures the session is closed after the test is completed.
    Yields:
       Session: A new database session for testing.
    """
    database = TestingSessionLocal()
    try:
        yield database
    finally:
        database.close()


app.dependency_overrides[get_db] = override_get_db


@pytest.fixture(scope="module")
def test_app():
    """Sets up and tears down the test database and test client.
    Creates all tables in the test database before yielding a test client for use in tests.
    Drops all tables in the test database after the tests are completed.

    Yields:
        TestClient: A test client for the FastAPI application.
    """
    Base.metadata.create_all(bind=engine)
    with TestClient(app) as client:
        yield client

    Base.metadata.drop_all(bind=engine)


@pytest.fixture(scope="function")
def test_session_local():
    """Provides a test-specific database session for individual test functions.
    This fixture creates a new database session using the `TestingSessionLocal` configuration,
    ensuring that each test function gets a fresh session. The session is automatically closed
    after the test function completes.
    Yields: Session: A new database session.
    """
    db = TestingSessionLocal()
    try:
        yield db
    finally:
        db.close


@pytest.fixture(scope="function")
def upload_single_employee_data(test_session_local):
    """
    Fixture to upload data for a single employee into the dummy_database.
    This function creates and commits new records for level, department, manager, project number,
    project code, project name, and an employee in the dummy_database. It ensures that all necessary
    foreign key relationships are properly established before adding the employee record.

    Args: test_session_local: SQLAlchemy database session for performing dummy_database operations.

    Returns: EmployeeData: The newly created employee record with all the details.
    """
    with test_session_local as db:

        new_level = LevelData(
            level_id=1,
            level="LAG2",
        )
        db.add(new_level)
        db.commit()
        db.refresh(new_level)

        new_department = DepartmentData(
            department_id=1,
            department="Engineering",
        )
        db.add(new_department)
        db.commit()
        db.refresh(new_department)

        new_manager = ManagerData(
            manager_id=1,
            manager="Yogesh Mann",
        )
        db.add(new_manager)
        db.commit()
        db.refresh(new_manager)

        new_project_number = ProjectNumberData(
            project_number_id=1,
            project_number="IN120",
        )
        db.add(new_project_number)
        db.commit()
        db.refresh(new_project_number)

        new_project_code = ProjectCodeData(
            project_code_id=1,
            project_code="IN120",
        )
        db.add(new_project_code)
        db.commit()
        db.refresh(new_project_code)

        new_project_name = ProjectNameData(
            project_name_id=1,
            project_name="SID",
        )
        db.add(new_project_name)
        db.commit()
        db.refresh(new_project_name)

        new_employee = EmployeeData(
            employee_id=1,
            indxx_id="IN345",
            hr_code="HR_001",
            first_name="John",
            last_name="Doe",
            start_date=date(2021, 1, 1),
            level_id=1,
            manager_id=1,
            department_id=1,
            project_number_id=1,
            project_code_id=1,
            project_name_id=1,
        )
        db.add(new_employee)
        db.commit()
        db.refresh(new_employee)
        return new_employee