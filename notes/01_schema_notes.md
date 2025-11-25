# Schema Notes — unified_customers

Primary key: id (string)
Important fields:
- name: string
- email: string (normalized, lowercase)
- signup_date: datetime (UTC)
- lifetime_value: float (USD)
- source_system: string

Notes:
- lifetime_value must be numeric; store None when unknown
- email should be validated; if not present, join by lead_id
- id should be stable across sources; prefer lead_id then email then order_id
