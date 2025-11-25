# Week 1 Summary - template

## What I did
- Created sample data for CRM and Sales
- Implemented `mappings.yml` to map to unified schema
- Built `etl/etl_merge.py` to run mapping-driven merge

## Issues found
- inconsistent date formats
- currency formatting ($ and commas)
- missing emails and unknown lead references

## Next steps / Blockers
- Confirm canonical join keys with team (email vs lead_id)
- Add fuzzy matching for names/emails when email missing
- Add unit tests and Great Expectations checks
