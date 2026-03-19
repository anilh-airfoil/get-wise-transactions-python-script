import os
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
    if s in {"pending", "incoming_payment_waiting", "processing", "funds_converted"}:
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
    if "deposit" in t:
        return "deposit"
    if "withdraw" in t:
        return "withdrawal"
    if "card" in t:
        return "card"
    return "other"

def normalize_direction(amount_value) -> str:
    try:
        amount = float(amount_value)
        return "incoming" if amount >= 0 else "outgoing"
    except Exception:
        return "outgoing"

def find_first_string(obj, candidate_keys: list[str]) -> str | None:
    if isinstance(obj, dict):
        for key in candidate_keys:
            val = obj.get(key)
            if isinstance(val, (str, int, float)) and str(val).strip():
                return str(val).strip()

        for _, value in obj.items():
            found = find_first_string(value, candidate_keys)
            if found:
                return found

    elif isinstance(obj, list):
        for item in obj:
            found = find_first_string(item, candidate_keys)
            if found:
                return found

    return None

# ========= WISE API =========
def get_profiles():
    # using v2 based on current docs screenshot
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
def find_transfer_id(tx: dict) -> str | None:
    candidate_keys = [
        "transferId",
        "transfer_id",
        "sourceTransferId",
        "recipientTransferId",
        "originalTransferId",
    ]
    return find_first_string(tx, candidate_keys)

def find_sender_name(tx: dict) -> str | None:
    candidate_keys = [
        "senderName",
        "payerName",
        "sourceName",
        "debtorName",
        "businessName",
    ]
    return find_first_string(tx, candidate_keys)

def find_recipient_name(tx: dict) -> str | None:
    candidate_keys = [
        "recipientName",
        "receiverName",
        "targetName",
        "creditorName",
        "merchantName",
        "counterpartyName",
    ]
    return find_first_string(tx, candidate_keys)

def build_n8n_payload(tx: dict, balance_currency: str, sync_time_iso: str, business_name: str) -> dict | None:
    tx_id = (
        find_first_string(tx, ["id", "transactionId", "referenceNumber", "reference"])
        or None
    )
    if not tx_id:
        return None

    amount_value = (
        safe_get(tx, "amount", "value")
        or tx.get("amount")
        or safe_get(tx, "totalAmount", "value")
        or safe_get(tx, "cashAmount", "value")
    )

    if isinstance(amount_value, dict):
        amount_value = amount_value.get("value")

    if amount_value is None:
        return None

    try:
        amount_float = float(amount_value)
    except Exception:
        return None

    amount_abs = abs(amount_float)
    direction = normalize_direction(amount_float)

    created_at = (
        tx.get("date")
        or tx.get("createdAt")
        or tx.get("created_at")
        or tx.get("bookingDate")
        or ""
    )

    raw_type = (
        tx.get("type")
        or tx.get("transactionType")
        or tx.get("detailsType")
        or "other"
    )

    raw_status = (
        tx.get("status")
        or tx.get("state")
        or "completed"
    )

    transfer_id = find_transfer_id(tx)
    sender_name = find_sender_name(tx) or business_name
    recipient_name = find_recipient_name(tx) or ""

    reference = (
        tx.get("reference")
        or tx.get("referenceNumber")
        or tx.get("description")
        or ""
    )

    source_currency = safe_get(tx, "amount", "currency") or balance_currency
    target_currency = balance_currency

    source_amount = amount_abs
    target_amount = amount_abs

    fee_value = (
        safe_get(tx, "fee", "value")
        or safe_get(tx, "totalFees", "value")
        or tx.get("fee")
        or 0
    )
    try:
        fee_value = abs(float(fee_value)) if fee_value is not None else 0
    except Exception:
        fee_value = 0

    payload = {
        "Transaction ID": str(tx_id),
        "Transfer ID": str(transfer_id) if transfer_id else "",
        "Status": normalize_status(raw_status),
        "Transaction Type": normalize_type(raw_type),
        "Direction": direction,
        "Amount": amount_abs,
        "Source Amount": source_amount,
        "Source Currency": source_currency,
        "Target Amount": target_amount,
        "Target Currency": target_currency,
        "Fee": fee_value,
        "Sender Name": sender_name,
        "Recipient Name": recipient_name,
        "Reference": str(reference),
        "Receipt URL": "",
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

    # n8n may return empty or json depending on your response node setup
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
