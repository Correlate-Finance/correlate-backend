from datetime import datetime, timedelta
from dateutil.parser import parse, ParserError
import re


VALID_DATE_PATTERNS = {
    # 2014Q1 -> Accepted pattern, do nothing
    r"^(\d{4})Q([1-4])": {
        "processor": lambda match: match.group(1) + "Q" + match.group(2),
        "year_extractor": lambda match: match.group(1),
    },
    # Q1'14 -> Convert to 2014Q1
    r"^Q([1-4])\'(\d{2})$": {
        "processor": lambda match: (
            convert_to_four_digit_year(match.group(2)) + "Q" + match.group(1)
        ),
        "year_extractor": lambda match: convert_to_four_digit_year(match.group(2)),
    },
}


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


def convert_to_four_digit_year(year: str, max_year: int = 2029):
    if int(year) < (max_year % 100):
        return "20" + year
    else:
        return "19" + year


def parse_year_from_date(date: str) -> int | None:
    """
    Given a date, returns the year of the date.
    """
    try:
        parsed_date = parse(date)
        return parsed_date.year
    except ParserError:
        pass

    # Try manual quarter passing
    for pattern, functors in VALID_DATE_PATTERNS.items():
        if match := re.match(pattern, date):
            return functors["year_extractor"](match)

    return None
