import requests
import json
import csv
import os
import re
from typing import Dict, List, Optional, Any
import logging

# --- CONFIG ---
BASE_URL = 'https://mademarket.co/api'
HEADERS = {
    'X-MM-API-KEY': '4DKARTF6YJR7',
    'Content-Type': 'application/json',
}
DISTRIBUTION_NAME = '2025 ISTE'
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- UTILITY FUNCTIONS ---
def safe_str(value: Any) -> str:
    if value is None:
        return ""
    if hasattr(value, 'size') and hasattr(value, '__getitem__'):
        try:
            if value.size == 0:
                return ""
            if value.size == 1:
                value = value.item()
            else:
                return ', '.join([safe_str(v) for v in value])
        except Exception:
            pass
    if isinstance(value, list):
        if len(value) == 0:
            return ""
        return ', '.join([safe_str(v) for v in value])
    try:
        import pandas as pd
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return str(value).strip()

def safe_get_nested(data: Dict, *keys, default=""):
    current = data
    for key in keys:
        if isinstance(current, dict) and key in current:
            current = current[key]
        else:
            return default
    return safe_str(current) if current is not None else default

def get_distribution_id(distribution_name):
    url = f"{BASE_URL}/v2/distributions/report_recipients.json"
    logger.info(f"[MadeMarket] GET {url}")
    response = requests.get(url, headers=HEADERS)
    logger.info(f"[MadeMarket] Response status: {response.status_code}")
    response.raise_for_status()
    data = response.json()
    for rec in data.get('distributions_recipients', []):
        dist = rec.get('distribution', {})
        if dist.get('name', '').lower() == distribution_name.lower():
            return dist.get('id')
    return None

def get_distribution_recipients(distribution_id):
    url = f"{BASE_URL}/v2/distributions/report_recipients.json"
    params = {"distribution_ids[]": distribution_id}
    logger.info(f"[MadeMarket] GET {url} | params: {params}")
    response = requests.get(url, headers=HEADERS, params=params)
    logger.info(f"[MadeMarket] Response status: {response.status_code}")
    response.raise_for_status()
    data = response.json()
    return data.get('distributions_recipients', [])

def get_contact_details(contact_id, contact_cache):
    if not contact_id:
        return None
    if contact_id in contact_cache:
        return contact_cache[contact_id]
    url = f"{BASE_URL}/v2/contacts/{contact_id}.json"
    logger.info(f"[MadeMarket] GET {url}")
    response = requests.get(url, headers=HEADERS)
    logger.info(f"[MadeMarket] Response status: {response.status_code}")
    if response.status_code != 200:
        return None
    data = response.json()
    contact_cache[contact_id] = data.get('contact', data)
    return contact_cache[contact_id]

def get_firm_details(firm_detail_id, firm_cache):
    if not firm_detail_id:
        return None
    if firm_detail_id in firm_cache:
        return firm_cache[firm_detail_id]
    url = f"{BASE_URL}/v2/firm_details/{firm_detail_id}.json"
    logger.info(f"[MadeMarket] GET {url}")
    response = requests.get(url, headers=HEADERS)
    logger.info(f"[MadeMarket] Response status: {response.status_code}")
    if response.status_code != 200:
        return None
    data = response.json()
    firm_cache[firm_detail_id] = data.get('firm_detail', data)
    return firm_cache[firm_detail_id]

def flatten_mademarket_row(recipient, contact, firm):
    row = {
        'recipient_email': safe_str(recipient.get('email')),
        'recipient_view_count': recipient.get('view_count', 0),
        'recipient_is_bounced': recipient.get('is_bounced', False),
        'email_opened': 1 if recipient.get('view_count', 0) > 0 else 0,
        'distribution_id': safe_get_nested(recipient, 'distribution', 'id'),
        'distribution_name': safe_get_nested(recipient, 'distribution', 'name'),
        'distribution_sent_at': safe_get_nested(recipient, 'distribution', 'sent_at'),
    }
    if contact:
        for k, v in contact.items():
            row[f'contact_{k}'] = safe_str(v)
    if firm:
        for k, v in firm.items():
            row[f'firm_{k}'] = safe_str(v)
    return row

if __name__ == "__main__":
    # --- Step 1: Pull and write main CSV ---
    output_dir = "Made_Market_Data"
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, f"mademarket_{DISTRIBUTION_NAME.replace(' ', '_')}.csv")

    dist_id = get_distribution_id(DISTRIBUTION_NAME)
    if not dist_id:
        print(f"Distribution '{DISTRIBUTION_NAME}' not found.")
        exit(1)
    recipients = get_distribution_recipients(dist_id)
    print(f"Found {len(recipients)} recipients.")
    contact_cache = {}
    firm_cache = {}
    rows = []
    for rec in recipients:
        contact = get_contact_details(rec.get('contact_id'), contact_cache)
        firm = get_firm_details(rec.get('firm_detail_id'), firm_cache)
        row = flatten_mademarket_row(rec, contact, firm)
        rows.append(row)
    if rows:
        with open(output_file, "w", newline='', encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
        print(f"Output written to {output_file}")
    else:
        print("No data to write.")
        exit(0)

    # --- Step 2: Segment opened/unopened ---
    opened_dir = os.path.join(output_dir, "opened")
    unopened_dir = os.path.join(output_dir, "unopened")
    os.makedirs(opened_dir, exist_ok=True)
    os.makedirs(unopened_dir, exist_ok=True)
    opened_file = os.path.join(opened_dir, f"mademarket_{DISTRIBUTION_NAME.replace(' ', '_')}_opened.csv")
    unopened_file = os.path.join(unopened_dir, f"mademarket_{DISTRIBUTION_NAME.replace(' ', '_')}_unopened.csv")

    opened = [row for row in rows if str(row.get('email_opened', '0')).strip() == '1']
    unopened = [row for row in rows if str(row.get('email_opened', '0')).strip() != '1']
    opened_sorted = sorted(opened, key=lambda r: r.get('recipient_email', '').lower())
    unopened_sorted = sorted(unopened, key=lambda r: r.get('recipient_email', '').lower())

    if opened_sorted:
        with open(opened_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=opened_sorted[0].keys())
            writer.writeheader()
            writer.writerows(opened_sorted)
    if unopened_sorted:
        with open(unopened_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=unopened_sorted[0].keys())
            writer.writeheader()
            writer.writerows(unopened_sorted)

    print(f"Segmented {len(opened_sorted)} opened and {len(unopened_sorted)} unopened.\nOpened: {opened_file}\nUnopened: {unopened_file}") 