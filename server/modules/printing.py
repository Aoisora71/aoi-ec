import json


def json_print(obj) -> None:
    try:
        print(json.dumps(obj, ensure_ascii=False, indent=2))
    except Exception:
        try:
            print(json.dumps(obj, ensure_ascii=True, indent=2))
        except Exception:
            print(str(obj))


def print_error(message: str) -> None:
    print(f" {message}")


