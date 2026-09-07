import re

_ALLOWED_START = re.compile(
    r"^\s*(SELECT|WITH|DECLARE|SET\s+ROWCOUNT)\b",
    re.IGNORECASE | re.DOTALL,
)
_FORBIDDEN = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE|EXEC(?:UTE)?|MERGE)\b",
    re.IGNORECASE,
)


def validate_query(sql: str) -> tuple[bool, str | None]:
    if not _ALLOWED_START.match(sql):
        return False, "Only SELECT / WITH / DECLARE statements are allowed."
    if _FORBIDDEN.search(sql):
        return False, "Statement contains a forbidden keyword (INSERT/UPDATE/DELETE/DROP/etc.)."
    return True, None
