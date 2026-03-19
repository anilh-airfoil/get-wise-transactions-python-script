import os
import re
import sys
import requests
from datetime import datetime, timedelta, timezone

# ========= ENV VARS =========
WISE_TOKEN = os.environ.get("WISE_TOKEN")
WISE_PROFILE_NAME = os.environ.get("WISE_PROFILE_NAME", "Airfoil")
WISE_LOOKBACK_DAYS = int(os.environ.get("WISE_LOOKBACK_DAYS", "30"))
N8N_WEBHOOK_URL = os.environ.get("N8N_WEBHOOK_URL")

if not WISE_TOKEN:
    raise ValueError("Missing WISE_TOKEN")
if not N8N_WEBHOOK_URL:
    raise ValueError("Missing N8N_WEBHOOK_URL")

# ========= CONFIG =========
WISE_HEADERS = {
    "Authorization": f"Bearer {WISE_TOKEN}",
    "Content-Type": "application/json",
    "Accept": "application/json",
}

TRANSFER_CACHE = {}
RECIPIENT_CACHE = {}

# ========= HELPERS =========
def log(msg: str):
    print(msg, flush=True)

def safe_get(dct, *keys, default=None):
    current = dct
    for key in keys:
        if not isinstance(current, dict):
            return default
        current = current.get(key)
        if current is None:
            return default
    return current

def to_iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def normalize_status(raw_status: str | None) -> str:
    if not raw_status:
        return "other"

    s = raw_status.lower()
    if s in {"completed", "done", "outgoing_payment_sent"}:
        return "completed"
    if s in {"pending", "incoming_payment_waiting", "processing", "funds_converted", "incoming_payment_initiated"}:
        return "processing"
    if s in {"cancelled", "canceled"}:
        return "cancelled"
    if s in {"failed", "bounced_back"}:
        return "failed"
    return "other"

def normalize_type(raw_type: str | None) -> str:
    if not raw_type:
        return "other"

    t = raw_type.lower()
    if "transfer" in t:
        return "transfer"
    if "conversion" in t or "convert" in t:
        return "conversion"
    if "fee" in t:
        return "fee"
    if "deposit" in t or "money_added" in t:
        return "deposit"
    if "withdraw" in t:
        return "withdrawal"
    if "card" in t:
        return "card"
    if "direct_debit" in t:
        return "direct_debit"
    return "other"

def normalize_direction(amount_value) -> str:
    try:
        amount = float(amount_value)
        return "incoming" if amount >= 0 else "outgoing"
    except Exception:
        return "outgoing"

def clean_iso_datetime(value: str | None) -> str:
    """
    Return a clean ISO8601 string Airtable/n8n can parse reliably.
    """
    if not value:
        return ""

    text = str(value).strip()
    if not text:
        return ""

    # Wise sometimes returns "2017-11-24 10:47:49"
    if "T" not in text and " " in text:
        try:
            dt = datetime.strptime(text, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            return to_iso_z(dt)
        except Exception:
            pass

    # Standard ISO strings, with or without Z
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return to_iso_z(dt)
    except Exception:
        return text

def first_non_empty(*values):
    for v in values:
        if v is None:
            continue
        if isinstance(v, str) and not v.strip():
            continue
        return v
    return None

def extract_numeric_transfer_id(value: str | None) -> str | None:
    """
    For refs like TRANSFER-2026433794 -> 2026433794
    """
    if not value:
        return None

    text = str(value).strip()
    m = re.search(r"TRANSFER-(\d+)", text, flags=re.IGNORECASE)
    if m:
        return m.group(1)

    if text.isdigit():
        return text

    return None

# ========= WISE API =========
def get_profiles():
    url = "https://api.wise.com/v2/profiles"
    resp = requests.get(url, headers=WISE_HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.json()

def pick_business_profile(profiles: list[dict]) -> dict:
    for profile in profiles:
        ptype = str(profile.get("type", "")).lower()
        if ptype != "business":
            continue

        details = profile.get("details", {}) or {}
        name = (
            details.get("name")
            or details.get("businessName")
            or details.get("legalName")
            or ""
        )

        if WISE_PROFILE_NAME.lower() in str(name).lower():
            return profile

    for profile in profiles:
        if str(profile.get("type", "")).lower() == "business":
            return profile

    raise RuntimeError("No business profile found in Wise account.")

def get_balances(profile_id: int):
    url = f"https://api.wise.com/v4/profiles/{profile_id}/balances"
    resp = requests.get(
        url,
        headers=WISE_HEADERS,
        params={"types": "STANDARD"},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()

def get_balance_statement(profile_id: int, balance_id: int, currency: str, start_iso: str, end_iso: str):
    url = f"https://api.wise.com/v1/profiles/{profile_id}/balance-statements/{balance_id}/statement.json"
    params = {
        "currency": currency,
        "intervalStart": start_iso,
        "intervalEnd": end_iso,
        "type": "COMPACT",
    }
    resp = requests.get(url, headers=WISE_HEADERS, params=params, timeout=60)
    resp.raise_for_status()
    return resp.json()

def get_transfer_by_id(transfer_id: str) -> dict | None:
    if not transfer_id:
        return None
    if transfer_id in TRANSFER_CACHE:
        return TRANSFER_CACHE[transfer_id]

    url = f"https://api.wise.com/v1/transfers/{transfer_id}"
    resp = requests.get(url, headers=WISE_HEADERS, timeout=30)

    if resp.status_code == 404:
        TRANSFER_CACHE[transfer_id] = None
        return None

    resp.raise_for_status()
    data = resp.json()
    TRANSFER_CACHE[transfer_id] = data
    return data

def get_recipient_account(account_id: int | str | None) -> dict | None:
    if not account_id:
        return None

    account_id = str(account_id)
    if account_id in RECIPIENT_CACHE:
        return RECIPIENT_CACHE[account_id]

    url = f"https://api.wise.com/v2/accounts/{account_id}"
    resp = requests.get(url, headers=WISE_HEADERS, timeout=30)

    if resp.status_code == 404:
        RECIPIENT_CACHE[account_id] = None
        return None

    resp.raise_for_status()
    data = resp.json()
    RECIPIENT_CACHE[account_id] = data
    return data

def extract_transactions(statement_json: dict) -> list[dict]:
    txs = statement_json.get("transactions")
    if isinstance(txs, list):
        return txs

    for key in ("statement", "data", "items"):
        maybe = statement_json.get(key)
        if isinstance(maybe, dict):
            inner = maybe.get("transactions")
            if isinstance(inner, list):
                return inner
        elif isinstance(maybe, list):
            return maybe

    return []

# ========= MAPPING =========
def find_transfer_reference(tx: dict) -> str | None:
    return first_non_empty(
        tx.get("referenceNumber"),
        safe_get(tx, "details", "referenceNumber"),
        tx.get("reference"),
        safe_get(tx, "details", "reference"),
    )

def find_sender_name(tx: dict, business_name: str) -> str:
    return first_non_empty(
        safe_get(tx, "details", "senderName"),
        tx.get("senderName"),
        business_name,
    ) or business_name

def find_card_or_description_name(tx: dict) -> str | None:
    merchant_name = first_non_empty(
        safe_get(tx, "details", "merchant", "name"),
        safe_get(tx, "details", "merchantName"),
    )
    if merchant_name:
        return str(merchant_name).strip()

    description = first_non_empty(
        safe_get(tx, "details", "description"),
        tx.get("description"),
    )
    if description:
        return str(description).strip()

    return None

def find_clean_recipient_name_from_account(recipient_account: dict | None) -> str:
    if not recipient_account or not isinstance(recipient_account, dict):
        return ""

    return str(first_non_empty(
        safe_get(recipient_account, "name", "fullName"),
        recipient_account.get("accountHolderName"),
        recipient_account.get("name"),
    ) or "").strip()

def build_n8n_payload(
    tx: dict,
    balance_currency: str,
    sync_time_iso: str,
    business_name: str,
) -> dict | None:
    # Statement fields per Wise docs
    tx_date = clean_iso_datetime(tx.get("date"))
    tx_amount_value = safe_get(tx, "amount", "value")
    tx_amount_currency = safe_get(tx, "amount", "currency") or balance_currency
    tx_fee_value = safe_get(tx, "totalFees", "value")
    tx_details = tx.get("details", {}) or {}
    tx_exchange_details = tx.get("exchangeDetails", {}) or {}

    if tx_amount_value is None:
        return None

    try:
        amount_float = float(tx_amount_value)
    except Exception:
        return None

    amount_abs = abs(amount_float)
    direction = normalize_direction(amount_float)

    raw_type = first_non_empty(
        safe_get(tx, "details", "type"),
        tx.get("type"),
        tx.get("transactionType"),
        tx.get("detailsType"),
        "other",
    )

    raw_status = first_non_empty(
        tx.get("status"),
        tx.get("state"),
        "completed",
    )

    reference = str(first_non_empty(
        safe_get(tx, "details", "paymentReference"),
        safe_get(tx, "details", "description"),
        tx.get("referenceNumber"),
        tx.get("reference"),
        ""
    ))

    transaction_id = str(first_non_empty(
        tx.get("referenceNumber"),
        tx.get("id"),
        reference,
    ) or "")

    if not transaction_id:
        return None

    source_amount = amount_abs
    source_currency = tx_amount_currency

    target_amount = None
    target_currency = None

    sender_name = find_sender_name(tx, business_name)
    recipient_name = ""

    # ===== Transfer enrichment =====
    transfer_numeric_id = extract_numeric_transfer_id(transaction_id) or extract_numeric_transfer_id(reference)
    transfer_obj = None

    if normalize_type(str(raw_type)) == "transfer" and transfer_numeric_id:
        try:
            transfer_obj = get_transfer_by_id(transfer_numeric_id)
        except Exception as e:
            log(f"Could not fetch transfer {transfer_numeric_id}: {e}")

    if transfer_obj:
        # Transfer object is the best source for transfer amounts/currencies
        source_amount = first_non_empty(transfer_obj.get("sourceValue"), source_amount)
        source_currency = first_non_empty(transfer_obj.get("sourceCurrency"), source_currency)
        target_amount = first_non_empty(transfer_obj.get("targetValue"), target_amount)
        target_currency = first_non_empty(transfer_obj.get("targetCurrency"), target_currency)

        # Better status and created timestamp if present
        raw_status = first_non_empty(transfer_obj.get("status"), raw_status)
        tx_date = clean_iso_datetime(first_non_empty(transfer_obj.get("created"), tx_date))

        target_account = transfer_obj.get("targetAccount")
        if target_account:
            try:
                recipient_account = get_recipient_account(target_account)
                recipient_name = find_clean_recipient_name_from_account(recipient_account)
            except Exception as e:
                log(f"Could not fetch recipient account {target_account}: {e}")

    # ===== Conversion fallback from balance statement =====
    if target_amount is None:
        target_amount = first_non_empty(
            safe_get(tx_details, "targetAmount", "value"),
            safe_get(tx_exchange_details, "forAmount", "value"),
        )

    if not target_currency:
        target_currency = first_non_empty(
            safe_get(tx_details, "targetAmount", "currency"),
            safe_get(tx_exchange_details, "forAmount", "currency"),
        )

    # ===== Card / non-transfer recipient fallback =====
    if not recipient_name:
        recipient_name = find_card_or_description_name(tx) or ""

        # Clean common noisy transfer descriptions
        if recipient_name.lower().startswith("sent money to "):
            recipient_name = recipient_name[14:].strip()
        elif recipient_name.lower().startswith("received money from "):
            recipient_name = recipient_name[20:].strip()

    # ===== Final sane fallbacks =====
    if source_amount is not None:
        try:
            source_amount = abs(float(source_amount))
        except Exception:
            source_amount = amount_abs
    else:
        source_amount = amount_abs

    if target_amount is not None:
        try:
            target_amount = abs(float(target_amount))
        except Exception:
            target_amount = source_amount
    else:
        target_amount = source_amount

    if not source_currency:
        source_currency = balance_currency

    if not target_currency:
        target_currency = source_currency

    try:
        fee_value = abs(float(tx_fee_value)) if tx_fee_value is not None else 0
    except Exception:
        fee_value = 0

    payload = {
        "Transaction ID": transaction_id,
        "Transaction On": tx_date,
        "Status": normalize_status(str(raw_status)),
        "Transaction Type": normalize_type(str(raw_type)),
        "Direction": direction,
        "Amount": amount_abs,
        "Source Amount": source_amount,
        "Source Currency": source_currency,
        "Target Amount": target_amount,
        "Target Currency": target_currency,
        "Fee": fee_value,
        "Sender Name": sender_name or "",
        "Recipient Name": recipient_name or "",
        "Reference": reference,
        "Last Synced": sync_time_iso,
    }

    return {k: v for k, v in payload.items() if v is not None}

# ========= N8N =========
def send_to_n8n(payload: dict):
    resp = requests.post(
        N8N_WEBHOOK_URL,
        json=payload,
        timeout=30,
    )
    resp.raise_for_status()

    if resp.content:
        try:
            return resp.json()
        except Exception:
            return resp.text
    return None

# ========= MAIN =========
def main():
    sync_now = datetime.now(timezone.utc)
    sync_now_iso = to_iso_z(sync_now)

    interval_end = sync_now
    interval_start = sync_now - timedelta(days=WISE_LOOKBACK_DAYS)

    interval_start_iso = to_iso_z(interval_start)
    interval_end_iso = to_iso_z(interval_end)

    log("Fetching Wise profiles...")
    profiles = get_profiles()
    profile = pick_business_profile(profiles)
    profile_id = profile["id"]

    details = profile.get("details", {}) or {}
    business_name = (
        details.get("name")
        or details.get("businessName")
        or details.get("legalName")
        or WISE_PROFILE_NAME
    )

    log(f"Using business profile ID: {profile_id}")
    log("Fetching balances...")
    balances = get_balances(profile_id)

    if not isinstance(balances, list):
        raise RuntimeError("Balances response is not a list.")

    sent_count = 0
    skipped_count = 0
    failed_count = 0

    for balance in balances:
        balance_id = balance.get("id")
        currency = balance.get("currency")

        if not balance_id or not currency:
            log(f"Skipping malformed balance: {balance}")
            continue

        log(f"Fetching statement for balance {balance_id} ({currency})...")
        statement = get_balance_statement(
            profile_id=profile_id,
            balance_id=balance_id,
            currency=currency,
            start_iso=interval_start_iso,
            end_iso=interval_end_iso,
        )

        transactions = extract_transactions(statement)
        log(f"Found {len(transactions)} transactions for {currency}")

        for tx in transactions:
            payload = build_n8n_payload(
                tx=tx,
                balance_currency=currency,
                sync_time_iso=sync_now_iso,
                business_name=business_name,
            )

            if not payload:
                skipped_count += 1
                continue

            try:
                send_to_n8n(payload)
                sent_count += 1
            except Exception as e:
                failed_count += 1
                log(f"Failed to send transaction {payload.get('Transaction ID')}: {e}")

    log("Sync completed.")
    log(f"Sent to n8n: {sent_count}")
    log(f"Skipped: {skipped_count}")
    log(f"Failed: {failed_count}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Fatal error: {e}", file=sys.stderr)
        raise
