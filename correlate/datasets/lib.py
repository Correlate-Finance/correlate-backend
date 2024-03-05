from datetime import datetime, timedelta


def get_date_from_days_since_1900(days_since_1900):
    """
    Given a number of days since January 1, 1900, returns the date for that number.
    """
    # Create a datetime object for January 1, 1900
    start_date = datetime(1900, 1, 1)

    # Add the given number of days to the start date
    # Excel has an error considering 1900 as a leap year, so subtract 2 instead of 1
    target_date = start_date + timedelta(days=days_since_1900 - 2)

    # Return the date as a datetime object
    return target_date
