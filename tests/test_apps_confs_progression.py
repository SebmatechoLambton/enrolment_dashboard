import sys
sys.path.append('C:\\Users\\c0846720\\OneDrive - Lambton College\\Desktop\\EnrolmentReport')
sys.path.append('C:\\Users\\c0846720\\OneDrive - Lambton College\\Documents')

import pytest
import pandas
from pandas._testing import assert_frame_equal
import pyodbc
import python_utils
from enrolment_utils import apps_confs_progression, probs_target_utils


@pytest.fixture
def mock_cnxn(mocker):
    """Fixture to mock the database connection."""
    return mocker.MagicMock(spec=pyodbc.Connection)

@pytest.fixture
def mock_dates_dict():
    """Fixture to mock dates dictionary returned by creating_dates_per_term."""
    return {
        '2023F': {
            'start_date': '2023-01-01',
            'end_date': '2023-04-30'
        }
    }

@pytest.fixture
def mock_dates_list():
    """Fixture to mock list of dates returned by getting_individual_dates."""
    return ['2023-01-01', '2023-01-02', '2023-01-03']

@pytest.fixture
def mock_df():
    """Fixture to create a mock dataframe."""
    return pandas.DataFrame({
        'ds': [pandas.Timestamp('2023-01-03')],
        'y': [10],
        'term': ['2023F']
    })
@pytest.fixture
def mock_df2():
    """Fixture to create a mock dataframe for a single day confirmation."""
    return pandas.DataFrame({
        'date': [pandas.Timestamp('2023-01-03')],
        'applications': [20],
        'previous_statuses': ['CCC CUC MTS']
    })

def test_retrieving_apps_program_per_term_per_date(mock_cnxn, mock_df, mocker):
    # Mock the pandas read_sql function
    mocker.patch('pandas.read_sql', return_value=mock_df)

    # Call the function with the mock connection and test data
    actual_df = apps_confs_progression.retrieving_apps_program_per_term_per_date(
        program='CDAS',
        term='2023F',
        date='2023-01-03',
        cnxn=mock_cnxn
    )

    # Assert that the returned DataFrame matches the expected mock DataFrame
    assert_frame_equal(actual_df, mock_df)

    # Assert that pandas.read_sql was called once
    pandas.read_sql.assert_called_once()


def test_building_program_record_apps(mock_cnxn, mock_dates_dict, mock_dates_list, mock_df, mocker):
    # Setup mocks
    mocker.patch.object(python_utils, 'get_connection', return_value=mock_cnxn)
    mocker.patch.object(probs_target_utils, 'creating_dates_per_term', return_value=mock_dates_dict)
    mocker.patch.object(probs_target_utils, 'getting_individual_dates', return_value=mock_dates_list)
    mock_retrieve = mocker.patch.object(apps_confs_progression, 'retrieving_apps_program_per_term_per_date', return_value=mock_df)
    
    # Call the function under test
    result_df = apps_confs_progression.building_program_record_apps(program='ACTG', 
                                                                    term='2023F', 
                                                                    start_year=2019, 
                                                                    end_year=2023, 
                                                                    cnxn=mock_cnxn)
    
    # Assert that the mock function was called with the correct arguments
    mock_retrieve.assert_called()
    
    # Assert that the returned DataFrame has the correct number of entries
    # This assumes that the number of entries should be equal to the number of dates in mock_dates_list
    assert len(result_df) == len(mock_dates_list)
    
    # Assert that the returned DataFrame has the expected columns
    assert list(result_df.columns) == ['ds', 'y', 'term']
    
    # Assert that the DataFrame is not empty
    assert not result_df.empty


def test_retrieving_confs_program_per_term_per_date(mock_cnxn, 
                                                    mock_df2, 
                                                    mocker):
    # Setup mocks
    mocker.patch.object(python_utils, 'get_connection', return_value=mock_cnxn)
    mocker.patch('pandas.read_sql', return_value=mock_df2)

    # Call the function under test
    actual_result = apps_confs_progression.retrieving_confs_program_per_term_per_date(program='CDAS', 
                                                                                      term='2023F', 
                                                                                      date='2023-01-03', 
                                                                                      cnxn=mock_cnxn)
    
    # Mocking the expected output
    expected_output = pandas.DataFrame({
        'ds': ['2023-01-03'],
        'y': [1],  # Assuming the mock_df2 contains one confirmation
        'term': ['2023F']
    })
    
    # Assertions to verify the expected outcome
    pandas.testing.assert_frame_equal(actual_result, expected_output)

    # Assert that pandas.read_sql was called with the correct parameters
    pandas.read_sql.assert_called_once()

def test_building_program_record_confs(mock_cnxn, mock_dates_dict, mock_dates_list, mock_df, mocker):
    # Setup mocks
    mocker.patch.object(python_utils, 'get_connection', return_value=mock_cnxn)
    mocker.patch.object(probs_target_utils, 'creating_dates_per_term', return_value=mock_dates_dict)
    mocker.patch.object(probs_target_utils, 'getting_individual_dates', return_value=mock_dates_list)
    mock_retrieve = mocker.patch.object(apps_confs_progression, 'retrieving_confs_program_per_term_per_date', return_value=mock_df)
    
    # Call the function under test
    result_df = apps_confs_progression.building_program_record_confs(program = 'ACTG', 
                                                                     term = '2023F',
                                                                     start_year = 2019, 
                                                                     end_year = 2023, 
                                                                     cnxn = mock_cnxn)
    
    # Construct the expected DataFrame
    expected_df = pandas.DataFrame({
        'ds': pandas.to_datetime(mock_dates_list),
        'y': [10, 10, 10],  # Assuming the mock_df contains 10 confirmations for each date
        'term': ['2023F', '2023F', '2023F']
    })
    
    # Assert the result is as expected
    assert_frame_equal(result_df.reset_index(drop=True), expected_df)

    # Verify the mock function was called the expected number of times
    assert mock_retrieve.call_count == len(mock_dates_list)

    # Verify that the function retrieves the data for the correct terms and dates
    for date in mock_dates_list:
        mock_retrieve.assert_any_call(program='ACTG',
                                      term='2023F', 
                                      date=date, 
                                      cnxn=mock_cnxn)