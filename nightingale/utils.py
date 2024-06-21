from datetime import datetime


def produce_package_name(date):
    return f"release-package-{date}.json"


def remove_dicts_without_id(data):
    if isinstance(data, dict):
        result = {}
        for k, val in data.items():
            if cleaned_v := remove_dicts_without_id(val):
                result[k] = cleaned_v
        return result
    elif isinstance(data, list):
        return [remove_dicts_without_id(item) for item in data if not (isinstance(item, dict) and "id" not in item)]
    else:
        return data


def get_iso_now():
    now = datetime.utcnow()
    return now.isoformat() + "Z"
