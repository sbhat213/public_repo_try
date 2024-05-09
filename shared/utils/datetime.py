import datetime
from dateutil.relativedelta import relativedelta
from settings import Settings
from datetime import timedelta


def year_month():
    today = datetime.datetime.today()
    # today = datetime.datetime(today.year, today.month, 1)
    return f"{today.year}-{today.month}"


def previous_year_month():
    today = datetime.datetime.today() - relativedelta(months=1)
    return f"{today.year}-{today.month}"


def is_previous_range(interval_days):
    date = utc_now() - timedelta(days=interval_days)
    if f"{date.year}-{date.month}" == previous_year_month():
        return True
    return False


def now():
    return utc_now()


def utc_now():
    dt = datetime.datetime.utcnow()
    return dt


def utc_now_string():
    dt = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S %Z')
    return dt


def date_today():
    return datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)


def str_to_date(date_time_str):
    if date_time_str:
        return datetime.datetime.strptime(date_time_str, '%Y-%m-%d %H:%M:%S %Z')
    else:
        return None


def date_to_str(date_time):
    if date_time:
        return date_time.strftime('%Y-%m-%d %H:%M:%S %Z')
    else:
        return None


def utc_date_diff_in_hour(first, second):
    difference = (first - second)
    duration_in_s = difference.total_seconds()
    hours = divmod(duration_in_s, 3600)[0]

    return hours


def date_diffrence_hour(first, second):
    return (first - second).seconds // 3600


def date_diffrence_minutes(first, second):
    return ((first - second).seconds // 60)%60


def date_diffrence_seconds(first, second):
    return (first - second).seconds


def date_diffrence_days(first, second):
    return (first - second).days




