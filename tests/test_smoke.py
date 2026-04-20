from config.models import CompanyConfig
from validation.layout import validate_layout_43_line


def test_smoke_imports_and_layout_guard():
    assert CompanyConfig.__name__ == "CompanyConfig"
    validate_layout_43_line(" " * 43)
