from datetime import datetime


def convert_to_datetime(date_value, param_name):
    """Helper function to validate and convert a date."""
    if isinstance(date_value, datetime):
        return date_value  # Already a datetime, return as is
    elif isinstance(date_value, str):
        try:
            return datetime.strptime(date_value, "%Y-%m-%d")  # Convert string to datetime
        except ValueError:
            raise ValueError(f"Invalid {param_name} format. Use 'YYYY-MM-DD'.")
    else:
        raise TypeError(f"{param_name} must be either a datetime object or a string in 'YYYY-MM-DD' format.")
