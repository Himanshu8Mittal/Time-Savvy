from src.db.model.models import *


def test_read_user(upload_single_employee_data, test_app):
    """Test the API endpoint 'read_user' by first uploading data into the test database
    and then retrieving it using the API.
    This function checks if the data returned by the 'read_user' API matches the data
    initially uploaded to the test database.

    Args:
        upload_single_employee_data (EmployeeData): Fixture that uploads a single employee's
                                                    data into the test database.
        test_app (TestClient): Test client for making API requests.
    Asserts:
        The status code of the API response is 200.
        The data returned by the API matches the data uploaded in the test database.
    """

    response = test_app.get(f"/users/{upload_single_employee_data.indxx_id}")
    assert response.status_code == 200

    data = response.json()

    assert data["indxx_id"] == upload_single_employee_data.indxx_id
    assert data["hr_code"] == upload_single_employee_data.hr_code
    assert data["first_name"] == upload_single_employee_data.first_name
    assert data["last_name"] == upload_single_employee_data.last_name
    assert data["start_date"] == upload_single_employee_data.start_date.isoformat()
    assert data["level_id"] == upload_single_employee_data.level_id
    assert data["department_id"] == upload_single_employee_data.department_id
    assert data["manager_id"] == upload_single_employee_data.manager_id
    assert data["project_number_id"] == upload_single_employee_data.project_number_id
    assert data["project_code_id"] == upload_single_employee_data.project_code_id
    assert data["project_name_id"] == upload_single_employee_data.project_name_id

