# Validation Notes

- Email validation: require '@' and domain; flag rows missing or invalid email
- Date parsing: parse ISO, US mm/dd/yyyy, and yyyy/mm/dd
- Money parsing: strip $ and commas; ensure non-negative if business rule requires
- Business rules:
  - lifetime_value >= 0 (flag negative)
  - signup_date <= now
- Suggested checks:
  - uniqueness of id
  - no duplicate rows with identical id + signup_date
