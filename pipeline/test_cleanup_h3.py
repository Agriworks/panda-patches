import pandas as pd
from pipeline.cleanup_h3 import calculate_h3_plotNorganic_calc
from tests.conftest import validate_data_frame_rows


def test_calculate_h3_plotNorganic_calc():
    # Create a sample dataframe for testing
    df = pd.read_csv("tests/pipeline/test_data/test_calculate_h3_plotNorganic_calc.csv")

    # Test that the function returns a dataframe
    assert isinstance(calculate_h3_plotNorganic_calc(df), pd.DataFrame)

    # Test that the function correctly classifies plots
    df = calculate_h3_plotNorganic_calc(df)
    # assert df.loc[0, "h3_plot1organic_calc"] == df.loc[0, "expected_h3_plot1organic_calc"]
    # assert df.loc[1, "h3_plot1organic_calc"] == df.loc[1, "expected_h3_plot1organic_calc"]
    # assert df.loc[2, "h3_plot1organic_calc"] == df.loc[2, "expected_h3_plot1organic_calc"]
    # assert df.loc[0, "h3_plot2organic_calc"] == df.loc[0, "expected_h3_plot2organic_calc"]
    # assert df.loc[1, "h3_plot2organic_calc"] == df.loc[1, "expected_h3_plot2organic_calc"]
    # assert df.loc[2, "h3_plot2organic_calc"] == df.loc[2, "expected_h3_plot2organic_calc"]
    # assert df.loc[0, "h3_plot3organic_calc"] == df.loc[0, "expected_h3_plot3organic_calc"]
    # assert df.loc[1, "h3_plot3organic_calc"] == df.loc[1, "expected_h3_plot3organic_calc"]
    # assert df.loc[2, "h3_plot3organic_calc"] == df.loc[2, "expected_h3_plot3organic_calc"]

    # Test that the function correctly classifies plots
    validate_data_frame_rows(df)
