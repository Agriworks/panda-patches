import pandas as pd
from pipeline.cleanup_h3 import calculate_h3_plotNorganic_calc, classify_h3_fullyorganic
from tests.conftest import validate_data_frame_rows


def test_calculate_h3_plotNorganic_calc():
    # Create a sample dataframe for testing
    df = pd.read_csv("tests/pipeline/test_data/test_calculate_h3_plotNorganic_calc.csv")

    # Test that the function returns a dataframe
    assert isinstance(calculate_h3_plotNorganic_calc(df), pd.DataFrame)

    # Test that the function correctly classifies plots
    df = calculate_h3_plotNorganic_calc(df)

    # Test that the function correctly classifies plots
    validate_data_frame_rows(df)


def test_classify_h3_fullyorganic():
    # Create a sample dataframe for testing
    df = pd.read_csv("tests/pipeline/test_data/test_classify_h3_fullyorganic.csv")

    # Test that the function returns a dataframe
    assert isinstance(classify_h3_fullyorganic(df), pd.DataFrame)

    # Test that the function correctly classifies plots
    df = classify_h3_fullyorganic(df)

    # Test that the function correctly classifies plots
    validate_data_frame_rows(df)
