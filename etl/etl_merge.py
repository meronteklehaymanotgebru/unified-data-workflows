#!/usr/bin/env python3
"""
Simple mapping-driven ETL to merge CRM leads + Sales Orders into a unified customers table.
Run: python etl/etl_merge.py
"""
import csv
import re
from pathlib import Path
from typing import Dict, Any, List

import pandas as pd
import yaml

ROOT = Path(__file__).parent.parent
DATA_DIR = ROOT / 'data' / 'sample'
MAPPINGS = ROOT / 'mappings.yml'

# tolerant money regex: remove anything not 0-9, dot, minus
CURRENCY_RE = re.compile(r"[^0-9.\-]")

def read_csv(path: Path) -> pd.DataFrame:
    # default pandas read_csv handles unquoted commas poorly in bad csvs;
    # if your CSV has commas inside cells, ensure they are quoted or use a better CSV writer.
    return pd.read_csv(path)

def load_mappings(path: Path) -> Dict[str, Any]:
    with open(path, 'r') as f:
        return yaml.safe_load(f)

def coerce_money(x):
    if pd.isna(x):
        return None
    if isinstance(x, (int, float)) and not pd.isna(x):
        return float(x)
    s = str(x)
    s = CURRENCY_RE.sub('', s)
    try:
        return float(s)
    except Exception:
        return None

def coerce_date(x):
    if pd.isna(x):
        return pd.NaT
    try:
        # try common ISO / pandas robust parse
        return pd.to_datetime(x, utc=True, errors='coerce')
    except Exception:
        return pd.NaT

def standardize_email(x):
    if pd.isna(x):
        return None
    s = str(x).strip().lower()
    return s if '@' in s else None

def apply_mapping(df_list: List[pd.DataFrame], mapping: Dict[str, Any]) -> pd.DataFrame:
    """
    df_list: [crm_df, sales_df]
    mapping: loaded from mappings.yml
    returns unified dataframe
    """
    crm, sales = df_list

    # canonicalize crm columns for lookup
    crm_filled = crm.fillna('')
    crm_by_lead = {}
    crm_by_email = {}
    if 'lead_id' in crm_filled.columns:
        for _, row in crm_filled.iterrows():
            lead = row.get('lead_id')
            if lead:
                crm_by_lead[str(lead).strip()] = row.to_dict()
    if 'email' in crm_filled.columns:
        for _, row in crm_filled.iterrows():
            email = row.get('email')
            if isinstance(email, str) and '@' in email:
                crm_by_email[email.strip().lower()] = row.to_dict()

    records = []

    # iterate sales orders and attempt to link to crm using lead_ref or email
    for _, s in (sales.fillna('')).iterrows():
        lead_ref = s.get('lead_ref', '')
        matched = None
        if lead_ref and str(lead_ref).strip() in crm_by_lead:
            matched = crm_by_lead[str(lead_ref).strip()]
        else:
            email = s.get('customer_email') or s.get('contact_email') or ''
            if email and '@' in str(email):
                matched = crm_by_email.get(str(email).strip().lower())

        rec = {}
        for target_field, cfg in mapping['unified_customers'].items():
            value_found = None
            for src in cfg['source']:
                src_val = None
                # priority: matched crm row > sales row > crm row (if not matched)
                if matched and src in matched and matched.get(src) not in (None, ''):
                    src_val = matched.get(src)
                elif src in s and s.get(src) not in (None, ''):
                    src_val = s.get(src)
                elif (not matched) and src in crm_filled.columns:
                    # fallback to crm row if available (non-matched)
                    v = crm_filled.loc[:, src] if src in crm_filled.columns else None
                    # we cannot pick a specific crm row here (we're inside a sales row)
                    # so skip this fallback in sales branch
                    src_val = None

                if src_val not in (None, ''):
                    # coerce
                    if cfg['type'] == 'float':
                        src_val = coerce_money(src_val)
                    elif cfg['type'] == 'date':
                        src_val = coerce_date(src_val)
                    elif cfg['type'] == 'string':
                        src_val = str(src_val).strip()
                    value_found = src_val
                    break
            rec[target_field] = value_found
        records.append(rec)

    # include CRM-only leads (no matching sales)
    for _, c in (crm.fillna('')).iterrows():
        # Skip if this CRM's email already included in records
        crm_email = standardize_email(c.get('email'))
        if crm_email and any(r.get('email') == crm_email for r in records if r.get('email')):
            continue

        rec = {}
        for target_field, cfg in mapping['unified_customers'].items():
            value_found = None
            for src in cfg['source']:
                if src in c and c.get(src) not in (None, ''):
                    val = c.get(src)
                    if cfg['type'] == 'float':
                        val = coerce_money(val)
                    elif cfg['type'] == 'date':
                        val = coerce_date(val)
                    elif cfg['type'] == 'string':
                        val = str(val).strip()
                    value_found = val
                    break
            rec[target_field] = value_found
        records.append(rec)

    df = pd.DataFrame(records)

    # Normalize emails and derive fields
    if 'email' in df.columns:
        df['email'] = df['email'].apply(standardize_email)

    if 'lifetime_value' in df.columns:
        df['lifetime_value'] = df['lifetime_value'].apply(coerce_money)

    # Ensure signup_date is datetime and UTC-aware (if not null)
    if 'signup_date' in df.columns:
        df['signup_date'] = pd.to_datetime(df['signup_date'], utc=True, errors='coerce')

    # Create a canonical id if missing (for example, use email or generated id)
    if 'id' in df.columns:
        df['id'] = df['id'].apply(lambda x: str(x).strip() if x not in (None, '') else None)
        # fill missing id by email
        df.loc[df['id'].isna(), 'id'] = df.loc[df['id'].isna(), 'email'].apply(lambda e: e or None)

    return df

def main():
    crm = read_csv(DATA_DIR / 'crm_leads.csv')
    sales = read_csv(DATA_DIR / 'sales_orders.csv')
    mapping = load_mappings(MAPPINGS)

    unified = apply_mapping([crm, sales], mapping)
    out = ROOT / 'output'
    out.mkdir(exist_ok=True)
    unified.to_csv(out / 'unified_customers.csv', index=False)
    print('Wrote', out / 'unified_customers.csv')
    # quick stats
    print('Rows:', len(unified))
    if 'email' in unified.columns:
        print('Unique emails:', unified['email'].nunique())

if __name__ == '__main__':
    main()
