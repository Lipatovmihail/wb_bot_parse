#!/usr/bin/env python3
"""
Мини-бот: выполняет синхронизацию WB_sellers → WB_sellers_updates
и отправляет статус в Telegram.
"""

import base64
import calendar
import html
import json
import os
import re
import sys
import time
import traceback
from datetime import datetime, timedelta, timezone, date
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import requests
import mysql.connector
from mysql.connector import Error


MYSQL_CONFIG = {
    "host": os.environ.get("MYSQL_HOST", "mysql-main"),
    "port": int(os.environ.get("MYSQL_PORT", "3306")),
    "user": os.environ.get("MYSQL_USER", "mvlipatov"),
    "password": os.environ.get("MYSQL_PASSWORD", "09ofefozQQ!!"),
    "database": os.environ.get("MYSQL_DATABASE", "mvlipatov"),
    "autocommit": True,
}

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "7745152075:AAFOlLbdK-TjA8OM0veZ9IPp5b_5G9w4G-U")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "-1002613685383")
TELEGRAM_THREAD_ID = os.environ.get("TELEGRAM_THREAD_ID")

WB_ORDERS_URL = "https://statistics-api.wildberries.ru/api/v1/supplier/orders"
WB_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36"
)
WB_PRODUCTS_URL = "https://content-api.wildberries.ru/content/v2/get/cards/list"
WB_CSV_HOST_PATH = "/data/csv/WB_orders_import.csv"
WB_CSV_MYSQL_PATH = "/var/lib/mysql-files/csv/WB_orders_import.csv"
WB_SALES_CSV_HOST_PATH = "/data/csv/WB_sales_import.csv"
WB_SALES_CSV_MYSQL_PATH = "/var/lib/mysql-files/csv/WB_sales_import.csv"
RIGHT_COOLDOWN_MINUTES = int(os.environ.get("WB_RIGHT_COOLDOWN_MINUTES", "30"))
PRODUCTS_COOLDOWN_MINUTES = int(os.environ.get("WB_PRODUCTS_COOLDOWN_MINUTES", "60"))
ADS_COOLDOWN_MINUTES = int(os.environ.get("WB_ADS_COOLDOWN_MINUTES", "60"))
AD_STATS_COOLDOWN_MINUTES = int(os.environ.get("WB_AD_STATS_COOLDOWN_MINUTES", "60"))
AD_STATS_RIGHT_COOLDOWN_MINUTES = 30

TOTAL_NMID_RIGHT_COOLDOWN_MINUTES = 30
TOTAL_NMID_CSV_HOST_PATH = "/data/csv/GS_RNP_total_metrics_nmid.csv"
TOTAL_NMID_CSV_MYSQL_PATH = "/var/lib/mysql-files/csv/GS_RNP_total_metrics_nmid.csv"
TOTAL_NMID_STOCK_HISTORY_CSV_HOST_PATH = "/data/csv/GS_RNP_total_metrics_nmid_stock_history.csv"
TOTAL_NMID_STOCK_HISTORY_CSV_MYSQL_PATH = "/var/lib/mysql-files/csv/GS_RNP_total_metrics_nmid_stock_history.csv"
TOTAL_NMID_AD_STATS_CSV_HOST_PATH = "/data/csv/GS_RNP_total_stats_daily.csv"
TOTAL_NMID_AD_STATS_CSV_MYSQL_PATH = "/var/lib/mysql-files/csv/GS_RNP_total_stats_daily.csv"
WB_PRODUCTS_CSV_HOST_PATH = "/data/csv/WB_products_import.csv"
WB_PRODUCTS_CSV_MYSQL_PATH = "/var/lib/mysql-files/csv/WB_products_import.csv"
WB_AD_LIST_URL = "https://advert-api.wildberries.ru/adv/v1/promotion/count"
WB_AD_LIST_CSV_HOST_PATH = "/data/csv/WB_ad_list_import.csv"
WB_AD_LIST_CSV_MYSQL_PATH = "/var/lib/mysql-files/csv/WB_ad_list_import.csv"
WB_AD_STATS_URL = "https://advert-api.wildberries.ru/adv/v3/fullstats"
WB_AD_STATS_CSV_HOST_PATH = "/data/csv/WB_ad_stats_import.csv"
WB_AD_STATS_CSV_MYSQL_PATH = "/var/lib/mysql-files/csv/WB_ad_stats_import.csv"
AD_STATS_ALLOWED_STATUSES = {7, 9, 11}
AD_STATS_LOOKBACK_DAYS = 180
AD_STATS_DEFAULT_LOOKBACK_DAYS = 15
AD_STATS_MAX_INTERVAL_DAYS = 31
AD_STATS_MAX_CAMPAIGNS_PER_REQUEST = 100
AD_STATS_RATE_LIMIT_REQUESTS_PER_MINUTE = 3
AD_STATS_RATE_INTERVAL_SECONDS = 30
AD_STATS_SKIP_APP_TYPE_ZERO = True
AD_STATS_MAX_RETRIES = 5
AD_STATS_RETRY_BASE_DELAY_SECONDS = 30

# AD EXPENSES constants
WB_AD_EXPENSES_URL = "https://advert-api.wildberries.ru/adv/v1/upd"
WB_AD_EXPENSES_CSV_HOST_PATH = "/data/csv/WB_ad_expenses_import.csv"
WB_AD_EXPENSES_CSV_MYSQL_PATH = "/var/lib/mysql-files/csv/WB_ad_expenses_import.csv"
AD_EXPENSES_LOOKBACK_DAYS = 180
AD_EXPENSES_DEFAULT_LOOKBACK_DAYS = 15
AD_EXPENSES_MAX_INTERVAL_DAYS = 30
AD_EXPENSES_RATE_INTERVAL_SECONDS = 1  # 1 запрос в секунду
AD_EXPENSES_CHUNK_DELAY_SECONDS = 2  # задержка между чанками
AD_EXPENSES_RIGHT_COOLDOWN_MINUTES = 30
AD_EXPENSES_MAX_RETRIES = 5
AD_EXPENSES_RETRY_BASE_DELAY_SECONDS = 5  # от 5 секунд с нарастающей

# STOCKS constants
WB_STOCKS_CREATE_URL = "https://seller-analytics-api.wildberries.ru/api/v1/warehouse_remains"
WB_STOCKS_STATUS_URL = "https://seller-analytics-api.wildberries.ru/api/v1/warehouse_remains/tasks/{task_id}/status"
WB_STOCKS_DOWNLOAD_URL = "https://seller-analytics-api.wildberries.ru/api/v1/warehouse_remains/tasks/{task_id}/download"
WB_STOCKS_CSV_HOST_PATH = "/data/csv/WB_stocks_import.csv"
WB_STOCKS_CSV_MYSQL_PATH = "/var/lib/mysql-files/csv/WB_stocks_import.csv"
STOCKS_RIGHT_COOLDOWN_MINUTES = 30
STOCKS_STATUS_CHECK_INTERVAL_SECONDS = 5  # проверка статуса раз в 5 секунд
STOCKS_MAX_STATUS_CHECKS = 120  # максимум 10 минут ожидания (120 * 5 сек)
STOCKS_MAX_RETRIES = 5
STOCKS_RETRY_BASE_DELAY_SECONDS = 5

MSK_OFFSET = timezone(timedelta(hours=3))
MS_DAY = timedelta(days=1)


def msk_now() -> datetime:
    return datetime.now(tz=timezone.utc).astimezone(MSK_OFFSET)


def ymd(date: datetime) -> str:
    return date.strftime("%Y-%m-%d")


def ymd_minus_days(days: int) -> str:
    return ymd(msk_now() - MS_DAY * days)


def first_ymd(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    value = value.strip()
    if len(value) < 10:
        return None
    candidate = value[:10]
    try:
        datetime.strptime(candidate, "%Y-%m-%d")
    except ValueError:
        return None
    return candidate


def sub_days_from_ymd(ymd_value: str, days: int) -> Optional[str]:
    try:
        dt = datetime.strptime(ymd_value, "%Y-%m-%d")
    except ValueError:
        return None
    result = dt - timedelta(days=days)
    return result.strftime("%Y-%m-%d")


def add_days_to_ymd(ymd_value: str, days: int) -> Optional[str]:
    try:
        dt = datetime.strptime(ymd_value, "%Y-%m-%d")
    except ValueError:
        return None
    result = dt + timedelta(days=days)
    return result.strftime("%Y-%m-%d")


def chunk_between(start_ymd: str, end_ymd: str, max_days: int = AD_STATS_MAX_INTERVAL_DAYS) -> List[Dict[str, str]]:
    try:
        start_dt = datetime.strptime(start_ymd, "%Y-%m-%d")
        end_dt = datetime.strptime(end_ymd, "%Y-%m-%d")
    except ValueError:
        return []

    if start_dt > end_dt:
        start_dt, end_dt = end_dt, start_dt

    chunks: List[Dict[str, str]] = []
    current_start = start_dt
    delta_max = timedelta(days=max_days - 1)

    while current_start <= end_dt:
        current_end = min(current_start + delta_max, end_dt)
        chunks.append(
            {
                "beginDate": current_start.strftime("%Y-%m-%d"),
                "endDate": current_end.strftime("%Y-%m-%d"),
            }
        )
        current_start = current_end + timedelta(days=1)

    return chunks


def format_date_short(value: Optional[str]) -> str:
    if not value:
        return "-"
    try:
        dt = datetime.strptime(value[:10], "%Y-%m-%d")
        return dt.strftime("%d.%m.%y")
    except ValueError:
        return value


def test_connection() -> Tuple[bool, str]:
    connection = None
    cursor = None
    try:
        connection = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM WB_sellers WHERE gs_rnp_access = 1")
        count_active = cursor.fetchone()[0]
        return True, f"Подключение успешно. Активных селлеров: {count_active}"
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None and connection.is_connected():
            connection.close()


def _telegram_call(endpoint: str, payload: Dict[str, Any], action: str) -> Optional[Dict[str, Any]]:
    max_attempts = 5
    base_delay = 2.0

    for attempt in range(1, max_attempts + 1):
        try:
            response = requests.post(
                endpoint,
                json=payload,
                timeout=10,
            )
            response.raise_for_status()
            try:
                return response.json()
            except ValueError:
                return None
        except requests.exceptions.HTTPError as http_err:
            status = http_err.response.status_code if http_err.response else None
            retryable = status in {429, 500, 502, 503, 504}
            print(
                f"[WB-bot] Telegram {action} failed (status={status}) attempt {attempt}/{max_attempts}: {http_err}",
                file=sys.stderr,
                flush=True,
            )
            if not retryable:
                break
        except Exception as exc:
            print(
                f"[WB-bot] Telegram {action} unexpected error attempt {attempt}/{max_attempts}: {exc}",
                file=sys.stderr,
                flush=True,
            )

        if attempt < max_attempts:
            delay = base_delay * attempt
            time.sleep(delay)

    return None


def send_to_telegram(message: str) -> Optional[Dict[str, Any]]:
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
        "disable_notification": True,
    }
    if TELEGRAM_THREAD_ID:
        payload["message_thread_id"] = TELEGRAM_THREAD_ID

    endpoint = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    return _telegram_call(endpoint, payload, "sendMessage")


def edit_telegram_message(message_id: int, message: str) -> Optional[Dict[str, Any]]:
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "message_id": message_id,
        "text": message,
        "parse_mode": "HTML",
        "disable_notification": True,
    }
    if TELEGRAM_THREAD_ID:
        payload["message_thread_id"] = TELEGRAM_THREAD_ID

    endpoint = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/editMessageText"
    return _telegram_call(endpoint, payload, "editMessageText")


def sync_active_list(cursor: mysql.connector.cursor.MySQLCursor) -> None:
    cursor.execute(
        """
        INSERT INTO WB_sellers_updates (
            seller_id,
            telegram_username,
            wb_api_nameseller,
            wb_api_brand,
            in_workrnp
        )
        SELECT
            seller_id,
            telegram_username,
            wb_api_nameseller,
            wb_api_brand,
            gs_rnp_access AS in_workrnp
        FROM WB_sellers
        WHERE gs_rnp_access = 1
        ON DUPLICATE KEY UPDATE
            telegram_username = VALUES(telegram_username),
            wb_api_nameseller = VALUES(wb_api_nameseller),
            wb_api_brand = VALUES(wb_api_brand),
            in_workrnp = VALUES(in_workrnp)
        """
    )


def fetch_active_sellers(cursor: mysql.connector.cursor.MySQLCursorDict) -> List[Dict]:
    cursor.execute(
        """
        SELECT
            s.seller_id,
            s.wb_api_key,
            s.wb_api_brand,
            su.orders_status
        FROM WB_sellers AS s
        JOIN WB_sellers_updates AS su
            ON su.seller_id = s.seller_id
        WHERE su.in_workrnp = 1
        """
    )
    rows = cursor.fetchall()
    result = []
    for row in rows:
        orders_status = row.get("orders_status")
        if isinstance(orders_status, str):
            try:
                row["orders_status"] = json.loads(orders_status)
            except json.JSONDecodeError:
                row["orders_status"] = None
        result.append(row)
    return result


def fetch_access_list(cursor: mysql.connector.cursor.MySQLCursorDict) -> List[Dict]:
    cursor.execute(
        """
        SELECT
            seller_id,
            telegram_username,
            wb_api_nameseller,
            wb_api_brand
        FROM WB_sellers
        WHERE gs_rnp_access = 1
        """
    )
    return cursor.fetchall()


def build_start_message(rows: List[Dict]) -> str:
    brand_lines: List[str] = []
    for row in rows:
        brand = (row.get("wb_api_brand") or row.get("seller_id") or "").strip()
        name = (row.get("wb_api_nameseller") or "").strip()
        brand_safe = html.escape(brand)
        name_safe = html.escape(name)
        brand_lines.append(f"▫️{brand_safe} | {name_safe}")

    lines_joined = "\n".join(brand_lines)
    return (
        "<b>Запускаю обновление  РНП</b>\n"
        f"<blockquote><b>В работу взяты {len(brand_lines)} брендов:</b>\n"
        f"{lines_joined}\n"
        "</blockquote>"
    )


def fetch_sales_sellers(cursor: mysql.connector.cursor.MySQLCursorDict) -> List[Dict]:
    cursor.execute(
        """
        SELECT
            s.seller_id,
            s.wb_api_key,
            s.wb_api_brand,
            su.sales_status
        FROM WB_sellers AS s
        JOIN WB_sellers_updates AS su
            ON su.seller_id = s.seller_id
        WHERE su.in_workrnp = 1
        """
    )
    rows = cursor.fetchall()
    result: List[Dict] = []
    for row in rows:
        status = row.get("sales_status")
        if isinstance(status, str):
            try:
                row["sales_status"] = json.loads(status)
            except json.JSONDecodeError:
                row["sales_status"] = None
        result.append(row)
    return result


def compute_sales_date_from_and_priority(
    sellers: Iterable[Dict],
) -> Tuple[List[Dict], List[Dict], str]:
    processed: List[Dict] = []
    per_seller: Dict[str, Dict] = {}
    threshold180 = ymd_minus_days(180)
    default_date_from = ymd_minus_days(15)

    for idx, raw in enumerate(sellers):
        seller_id = str(raw.get("seller_id") or "").strip()
        if not seller_id:
            continue

        sales_status = raw.get("sales_status") or None
        brand = (raw.get("wb_api_brand") or seller_id).strip()

        status_cat: str
        date_from: str

        now_time: Optional[str] = None

        if not sales_status:
            status_cat = "new"
            date_from = default_date_from
        else:
            status_raw = sales_status.get("status")
            rb_ymd = first_ymd(sales_status.get("rightBoundary"))
            last_df = first_ymd(sales_status.get("lastDateFrom"))
            max_df = first_ymd(sales_status.get("maxDateFrom"))
            now_time = sales_status.get("nowTime")

            if status_raw == "right":
                candidate = sub_days_from_ymd(rb_ymd, 15) if rb_ymd else None
                date_from = candidate or last_df or max_df or default_date_from
            elif status_raw == "left":
                if max_df and max_df > threshold180:
                    date_from = threshold180
                elif max_df and max_df <= threshold180:
                    date_from = rb_ymd or threshold180 or default_date_from
                else:
                    date_from = last_df or max_df or default_date_from
            else:
                date_from = last_df or max_df or default_date_from

            if status_raw == "left":
                status_cat = "left"
            elif status_raw == "right":
                status_cat = "right"
            else:
                status_cat = "other"

        bucket = per_seller.get(seller_id)
        if bucket is None:
            bucket = {
                "seller_id": seller_id,
                "brand": brand or seller_id,
                "status_cat": status_cat,
                "date_from": date_from,
                "idx": idx,
                "sales_status": sales_status,
                "now_time": now_time,
            "cooldown_blocked": False,
            }
            per_seller[seller_id] = bucket
        else:
            rank = {"other": 0, "right": 1, "left": 2, "new": 3}
            if rank[status_cat] > rank[bucket["status_cat"]]:
                bucket["status_cat"] = status_cat
            if not bucket.get("date_from"):
                bucket["date_from"] = date_from
            if not bucket.get("brand"):
                bucket["brand"] = brand
            if not bucket.get("now_time") and now_time:
                bucket["now_time"] = now_time
        if "cooldown_blocked" not in bucket:
            bucket["cooldown_blocked"] = False

        processed.append(
            {
                "seller_id": seller_id,
                "brand": brand or seller_id,
                "status_cat": status_cat,
                "date_from": date_from,
                "idx": idx,
                "wb_api_key": raw.get("wb_api_key"),
                "sales_status": sales_status,
                "cooldown_blocked": False,
            }
        )

    all_sellers = list(per_seller.values())

    new_group = [s for s in all_sellers if s["status_cat"] == "new"]
    left_group = [s for s in all_sellers if s["status_cat"] == "left"]
    right_group = [s for s in all_sellers if s["status_cat"] == "right"]

    allow_set = set()

    if new_group:
        allow_set.update(s["seller_id"] for s in new_group)
    elif left_group:
        left_group.sort(key=lambda s: _parse_now_time((s.get("sales_status") or {}).get("nowTime")))
        allow_set.add(left_group[0]["seller_id"])
    elif right_group and len(right_group) == len(all_sellers):
        eligible_rights = []
        for s in right_group:
            status_dict = s.get("sales_status") or {}
            if right_status_is_ready(status_dict):
                eligible_rights.append(s)
            else:
                s["cooldown_blocked"] = True
        if eligible_rights:
            allow_set.update(s["seller_id"] for s in eligible_rights)

    for s in all_sellers:
        s["allowed"] = s["seller_id"] in allow_set

    lines = []
    for s in sorted(all_sellers, key=lambda item: item["brand"].lower()):
        mark = "✅" if s["allowed"] else "✖️"
        try:
            dt_obj = datetime.strptime(s["date_from"], "%Y-%m-%d")
            date_short = dt_obj.strftime("%d.%m.%y")
        except Exception:
            date_short = s["date_from"]
        now_dt = parse_msk_datetime((s.get("orders_status") or {}).get("nowTime") or s.get("now_time"))
        minutes_text = "--"
        if now_dt is not None:
            age_min = max(0, int(minutes_since_msk(now_dt)))
            minutes_text = f"{age_min}min"
        lines.append(f"{s['status_cat']} | dtFrm: {date_short} | {minutes_text} | {s['brand']} {mark}")
    summary_text = "\n".join(lines)

    allowed_items: List[Dict] = []
    for item in processed:
        if item["seller_id"] in allow_set:
            enriched = dict(item)
            enriched["text"] = summary_text
            allowed_items.append(enriched)

    return allowed_items, all_sellers, summary_text


def build_sales_selection_message(summary_text: str) -> str:
    escaped = html.escape(summary_text)
    return (
        "<b>02 WB API</b> | Sales\n"
        "<blockquote>Подготовка выгрузки продаж.\n"
        f"<code>{escaped}</code></blockquote>"
    )


def fetch_sales_for_seller(item: Dict) -> List[Dict]:
    params = {"dateFrom": item["date_from"]}
    headers = {
        "user-agent": WB_USER_AGENT,
        "Authorization": f"Bearer {item.get('wb_api_key', '').strip()}",
    }
    response = requests.get("https://statistics-api.wildberries.ru/api/v1/supplier/sales", params=params, headers=headers, timeout=30)
    if response.status_code != 200:
        raise RuntimeError(
            f"WB Sales API error for {item['seller_id']}: {response.status_code} {response.text}"
        )
    data = response.json()
    if not isinstance(data, list):
        raise RuntimeError(
            f"WB Sales API unexpected response for {item['seller_id']}: {data!r}"
        )
    enriched: List[Dict] = []
    for row in data:
        payload = dict(row)
        payload["seller_id"] = item["seller_id"]
        payload["wb_api_brand"] = item.get("brand")
        payload["dateFrom"] = item["date_from"]
        payload["wb_api_key"] = item.get("wb_api_key")
        payload["sales_status"] = item.get("sales_status")
        enriched.append(payload)
    return enriched


def build_products_request_body(limit: int, nm_id: int = 0, updated_at: str = "", with_photo: int = -1) -> Dict:
    cursor = {"limit": limit}
    if nm_id:
        cursor["nmID"] = nm_id
    if updated_at:
        cursor["updatedAt"] = updated_at
    return {
        "settings": {
            "cursor": cursor,
            "filter": {"withPhoto": with_photo},
        }
    }


def fetch_products_for_seller(item: Dict) -> List[Dict]:
    seller_id = item.get("seller_id") or item.get("sellerId") or ""
    token = (item.get("wb_api_key") or item.get("wb_api") or "").strip()
    if not token:
        raise RuntimeError(f"WB products token missing for seller {seller_id}")

    headers = {
        "user-agent": WB_USER_AGENT,
        "Authorization": f"Bearer {token}",
    }

    limit = 100
    nm_id = 0
    updated_at = ""
    with_photo = -1
    collected_cards: List[Dict] = []
    safety_counter = 0

    while True:
        body = build_products_request_body(limit=limit, nm_id=nm_id, updated_at=updated_at, with_photo=with_photo)
        response = requests.post(WB_PRODUCTS_URL, headers=headers, json=body, timeout=30)
        if response.status_code != 200:
            raise RuntimeError(
                f"WB Products API error for {seller_id}: {response.status_code} {response.text}"
            )
        data = response.json()
        cards_batch = data.get("cards") or []
        collected_cards.extend(cards_batch)

        cursor_info = data.get("cursor") or {}
        total = cursor_info.get("total")
        next_nm_id = cursor_info.get("nmID") or 0
        next_updated_at = cursor_info.get("updatedAt") or ""

        safety_counter += 1
        if (
            not cursor_info
            or total is None
            or total < limit
            or (next_nm_id == 0 and not next_updated_at)
            or safety_counter > 100
        ):
            break

        nm_id = next_nm_id
        updated_at = next_updated_at

    return collected_cards


def build_sales_summary(items: List[Dict]) -> str:
    def first_ymd(value: Optional[str]) -> Optional[str]:
        if not value:
            return None
        value = str(value)
        candidate = value[:10]
        try:
            datetime.strptime(candidate, "%Y-%m-%d")
            return candidate
        except ValueError:
            return None

    def min_str(a: Optional[str], b: Optional[str]) -> Optional[str]:
        if a is None:
            return b
        if b is None:
            return a
        return a if a < b else b

    def max_str(a: Optional[str], b: Optional[str]) -> Optional[str]:
        if a is None:
            return b
        if b is None:
            return a
        return a if a > b else b

    def fmt_int(value: int) -> str:
        return f"{value:,}".replace(",", " ")

    per_seller: Dict[str, Dict[str, Optional[str]]] = {}
    for row in items:
        seller_id = str(row.get("seller_id") or "").strip() or "unknown"
        brand = (str(row.get("wb_api_brand") or "")).strip() or seller_id
        ymd_val = first_ymd(row.get("date")) or first_ymd(row.get("lastChangeDate"))

        bucket = per_seller.get(seller_id)
        if bucket is None:
            bucket = {"brand": brand, "min": None, "max": None, "cnt": 0}
            per_seller[seller_id] = bucket

        bucket["cnt"] = int(bucket.get("cnt") or 0) + 1
        bucket["brand"] = bucket.get("brand") or brand
        if ymd_val:
            bucket["min"] = min_str(bucket.get("min"), ymd_val)
            bucket["max"] = max_str(bucket.get("max"), ymd_val)

    lines = []
    for bucket in sorted(per_seller.values(), key=lambda b: b["brand"]):
        line = (
            f"{bucket.get('min') or '-'} · "
            f"{bucket.get('max') or '-'} · "
            f"{fmt_int(bucket.get('cnt') or 0)} · "
            f"{bucket.get('brand')}"
        )
        lines.append(line)

    return "\n".join(lines)


def build_status_update_message(prefix: str, entity: str, details: List[Dict]) -> str:
    total_rows = sum(d.get("count", 0) or 0 for d in details)
    header = f"<b>{prefix} WB API</b> | {entity}\n<blockquote>Статус обновлён ✅"
    summary_line = f"Всего селлеров: {len(details)} | всего строк: {total_rows}"
    body_lines = []
    for d in details:
        date_short = d.get("date_from") or ""
        try:
            if date_short:
                date_short = datetime.strptime(date_short, "%Y-%m-%d").strftime("%d.%m.%y")
        except Exception:
            pass
        from_status = d.get("from") or "null"
        to_status = d.get("to") or "null"
        brand = html.escape(d.get("brand") or d.get("seller_id") or "")
        count = d.get("count") or 0
        body_lines.append(f"{date_short} | {from_status} → {to_status} | {count} | {brand}")
    closing = "</blockquote>"
    return "\n".join([header, summary_line, *body_lines, closing])


def build_products_status_message(details: List[Dict]) -> str:
    header = "<b>03 WB API</b> | Products\n<blockquote>Статус обновлён ✅"
    summary_line = f"Всего селлеров: {len(details)}"
    lines = []
    for item in details:
        brand_name = html.escape(item.get("brand") or item.get("seller_id") or "")
        status = item.get("status") or {}

        line = f"nmID:{status.get('nmID', 0)} | skus:{status.get('skus', 0)} | {brand_name}"
        lines.append(html.escape(line))

    body = "<code>" + ("\n".join(lines) if lines else "нет данных") + "</code>"
    closing = "</blockquote>"
    return "\n".join([header, summary_line, body, closing])


def build_ad_list_status_message(details: List[Dict]) -> str:
    header = "<b>04 WB API</b> | Ad List\n<blockquote>Статус обновлён ✅"
    summary_line = f"Всего селлеров: {len(details)}"
    lines = []
    for item in details:
        brand_name = html.escape(item.get("brand") or item.get("seller_id") or "")
        status = item.get("status") or {}
        line = (
            f"all:{status.get('ad_all_cnt', 0)} | "
            f"active:{status.get('ad_active_cnt', 0)} | "
            f"paused:{status.get('ad_paused_cnt', 0)} | "
            f"{brand_name}"
        )
        lines.append(html.escape(line))

    body = "<code>" + ("\n".join(lines) if lines else "нет данных") + "</code>"
    closing = "</blockquote>"
    return "\n".join([header, summary_line, body, closing])


def build_stocks_status_message(details: List[Dict]) -> str:
    header = "<b>07 WB API</b> | STOCKS\n<blockquote>Статус обновлён ✅"
    summary_line = f"Всего селлеров: {len(details)}"
    lines = []

    def fmt_int(value: Any) -> str:
        try:
            number = int(value)
        except Exception:
            number = 0
        return f"{number:,}".replace(",", " ")

    for item in sorted(details, key=lambda x: (x.get("brand") or x.get("seller_id") or "").lower()):
        brand_name = html.escape(item.get("brand") or item.get("seller_id") or "")
        status = item.get("status") or {}
        line = (
            f"nmIDs:{status.get('nmid_stock_cnt', 0)} | "
            f"all_stock:{fmt_int(status.get('all_stock_cnt', 0))} | "
            f"{brand_name}"
        )
        lines.append(html.escape(line))

    body = "<code>" + ("\n".join(lines) if lines else "нет данных") + "</code>"
    closing = "</blockquote>"
    return "\n".join([header, summary_line, body, closing])


def build_ad_stats_status_message(details: List[Dict]) -> str:
    header = "<b>05 WB API</b> | Ad Stats\n<blockquote>Статус обновлён ✅"
    total_sellers = len(details)
    total_rows = sum(item.get("count", 0) for item in details)
    summary_line = f"Всего селлеров: {total_sellers} | всего строк: {total_rows}"
    lines = []
    for item in sorted(details, key=lambda x: (x.get("brand") or x.get("seller_id") or "").lower()):
        brand_name = html.escape(item.get("brand") or item.get("seller_id") or "")
        from_status = item.get("from", "null")
        to_status = item.get("to", "null")
        count = item.get("count", 0)
        max_begin_date = item.get("maxBeginDate", "")
        line = f"{max_begin_date or 'null'} | {from_status} → {to_status} | {count} | {brand_name}"
        lines.append(html.escape(line))

    body = "<code>" + ("\n".join(lines) if lines else "нет данных") + "</code>"
    closing = "</blockquote>"
    return "\n".join([header, summary_line, body, closing])


def build_completion_message(prefix: str, entity: str, elapsed_seconds: int) -> str:
    safe_elapsed = max(0, int(elapsed_seconds))
    minutes, seconds = divmod(safe_elapsed, 60)
    return (
        f"<b>{prefix} WB API</b> | {entity} завершен ✅\n"
        f"<blockquote>Время: {minutes} мин {seconds} сек</blockquote>"
    )


PRODUCTS_CSV_COLUMNS = [
    "seller_id",
    "nmID",
    "imtID",
    "vendorCode",
    "subjectID",
    "subjectName",
    "brand",
    "title",
    "description",
    "photo",
    "photo_high",
    "video",
    "dimensions_length",
    "dimensions_width",
    "dimensions_height",
    "weightBrutto",
    "dimensions_isValid",
    "characteristics",
    "tags",
    "skus",
    "techSize",
    "createdAt",
    "updatedAt",
    "needKiz",
]

AD_LIST_CSV_COLUMNS = [
    "seller_id",
    "advert_seller_key",
    "advertId",
    "type",
    "status",
    "changeTime",
]

AD_STATS_CSV_COLUMNS = [
    "seller_advert_date_key",
    "seller_id",
    "advertId",
    "date",
    "nmId",
    "appType",
    "views",
    "clicks",
    "ctr",
    "cpc",
    "ad_expenses",
    "atbs",
    "orders",
    "shks",
    "cr",
    "canceled",
    "sum",
    "sum_price",
    "avg_position",
]

AD_EXPENSES_CSV_COLUMNS = [
    "seller_advert_date_key",
    "seller_id",
    "advertId",
    "campName",
    "advertType",
    "paymentType",
    "advertStatus",
    "updNum",
    "updTime",
    "updSum",
]

STOCKS_CSV_COLUMNS = [
    "seller_bc_key",
    "seller_id",
    "barcode",
    "subjectName",
    "vendorCode",
    "nmId",
    "techSize",
    "volume",
    "quantity",
    "inWayToClient",
    "inWayFromClient",
    "quantityFull",
    "warehouses",
]


def parse_json_field(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        candidate = value.strip()
        if not candidate:
            return None
        try:
            return json.loads(candidate)
        except json.JSONDecodeError:
            return None
    return None


def parse_ymd_date(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    try:
        return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()
    except Exception:
        return None


def first_day_of_month(dt: date) -> date:
    return date(dt.year, dt.month, 1)


def add_months_first_day(dt: date, months: int) -> date:
    month_index = dt.month - 1 + months
    year = dt.year + month_index // 12
    month = month_index % 12 + 1
    return date(year, month, 1)


def last_day_of_month(dt: date) -> date:
    _, last_day = calendar.monthrange(dt.year, dt.month)
    return date(dt.year, dt.month, last_day)


def month_span(start: date, end: date) -> Iterable[date]:
    if start > end:
        return []
    current = date(start.year, start.month, 1)
    final = date(end.year, end.month, 1)
    span: List[date] = []
    while current <= final:
        span.append(current)
        current = add_months_first_day(current, 1)
    return span


def to_ymd_from_iso(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    candidate = str(value).strip()
    if not candidate:
        return None
    if "T" in candidate:
        candidate = candidate.split("T", 1)[0]
    return first_ymd(candidate)


def to_int_or_none(value: Any) -> Optional[int]:
    try:
        value_int = int(value)
    except (TypeError, ValueError):
        return None
    return value_int


def extract_nm_id(entry: Dict[str, Any]) -> Optional[int]:
    for key in ("nmId", "nm_id", "nm", "nmid"):
        if key in entry:
            return to_int_or_none(entry.get(key))
    return None


def build_booster_index(entries: Any) -> Dict[str, Any]:
    index: Dict[str, Any] = {}
    if not isinstance(entries, list):
        return index
    for item in entries:
        if not isinstance(item, dict):
            continue
        date_str = to_ymd_from_iso(item.get("date"))
        nm_id = extract_nm_id(item)
        if date_str and nm_id is not None:
            index[f"{date_str}|{nm_id}"] = item.get("avg_position")
    return index


def iso_to_mysql(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    match = re.match(r"^(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2}:\d{2})", str(value))
    if match:
        return f"{match.group(1)} {match.group(2)}"
    return None


def pick_photos(photos: Optional[List[Dict]]) -> Tuple[str, str]:
    if not photos:
        return "", ""
    photo_entry = photos[0] or {}
    high = (
        photo_entry.get("big")
        or photo_entry.get("hq")
        or photo_entry.get("c516x688")
        or photo_entry.get("c246x328")
        or photo_entry.get("square")
        or photo_entry.get("tm")
        or ""
    )
    small = (
        photo_entry.get("c246x328")
        or photo_entry.get("big")
        or photo_entry.get("hq")
        or photo_entry.get("c516x688")
        or photo_entry.get("square")
        or photo_entry.get("tm")
        or ""
    )
    return small, high


def convert_products_for_csv(rows: Iterable[Dict]) -> List[Dict]:
    csv_rows: List[Dict] = []
    for row in rows:
        cards = row.get("cards") or []
        seller_id = row.get("seller_id") or row.get("sellerId") or row.get("seller_id") or "unknown"
        for card in cards:
            nmID = card.get("nmID")
            imtID = card.get("imtID")
            vendorCode = card.get("vendorCode")
            subjectID = card.get("subjectID")
            subjectName = card.get("subjectName")
            brand = card.get("brand")
            title = card.get("title")
            description = card.get("description")
            photo, photo_high = pick_photos(card.get("photos"))

            dimensions = card.get("dimensions") or {}
            lengthVal = dimensions.get("length")
            widthVal = dimensions.get("width")
            heightVal = dimensions.get("height")
            weightBruttoVal = dimensions.get("weightBrutto")
            isValidVal = 1 if dimensions.get("isValid") else 0

            characteristics = json.dumps(card.get("characteristics") or [])
            tags = json.dumps(card.get("tags") or [])

            created_at = iso_to_mysql(card.get("createdAt"))
            updated_at = iso_to_mysql(card.get("updatedAt"))
            needKiz = 1 if card.get("needKiz") else 0
            video = card.get("video") or ""

            sizes = card.get("sizes") or []
            if sizes:
                for sizeVariant in sizes:
                    techSize = sizeVariant.get("techSize") or ""
                    skus = sizeVariant.get("skus") or []
                    if skus:
                        for sku in skus:
                            csv_rows.append(
                                {
                                    "seller_id": seller_id,
                                    "nmID": nmID,
                                    "imtID": imtID,
                                    "vendorCode": vendorCode,
                                    "subjectID": subjectID,
                                    "subjectName": subjectName,
                                    "brand": brand,
                                    "title": title,
                                    "description": description,
                                    "photo": photo,
                                    "photo_high": photo_high,
                                    "video": video,
                                    "dimensions_length": lengthVal,
                                    "dimensions_width": widthVal,
                                    "dimensions_height": heightVal,
                                    "weightBrutto": weightBruttoVal,
                                    "dimensions_isValid": isValidVal,
                                    "characteristics": characteristics,
                                    "tags": tags,
                                    "skus": sku,
                                    "techSize": techSize,
                                    "createdAt": created_at,
                                    "updatedAt": updated_at,
                                    "needKiz": needKiz,
                                }
                            )
                    else:
                        csv_rows.append(
                            {
                                "seller_id": seller_id,
                                "nmID": nmID,
                                "imtID": imtID,
                                "vendorCode": vendorCode,
                                "subjectID": subjectID,
                                "subjectName": subjectName,
                                "brand": brand,
                                "title": title,
                                "description": description,
                                "photo": photo,
                                "photo_high": photo_high,
                                "video": video,
                                "dimensions_length": lengthVal,
                                "dimensions_width": widthVal,
                                "dimensions_height": heightVal,
                                "weightBrutto": weightBruttoVal,
                                "dimensions_isValid": isValidVal,
                                "characteristics": characteristics,
                                "tags": tags,
                                "skus": None,
                                "techSize": techSize,
                                "createdAt": created_at,
                                "updatedAt": updated_at,
                                "needKiz": needKiz,
                            }
                        )
            else:
                csv_rows.append(
                    {
                        "seller_id": seller_id,
                        "nmID": nmID,
                        "imtID": imtID,
                        "vendorCode": vendorCode,
                        "subjectID": subjectID,
                        "subjectName": subjectName,
                        "brand": brand,
                        "title": title,
                        "description": description,
                        "photo": photo,
                        "photo_high": photo_high,
                        "video": video,
                        "dimensions_length": lengthVal,
                        "dimensions_width": widthVal,
                        "dimensions_height": heightVal,
                        "weightBrutto": weightBruttoVal,
                        "dimensions_isValid": isValidVal,
                        "characteristics": characteristics,
                        "tags": tags,
                        "skus": None,
                        "techSize": "",
                        "createdAt": created_at,
                        "updatedAt": updated_at,
                        "needKiz": needKiz,
                    }
                )

    return [row for row in csv_rows if row.get("skus")]


def write_products_csv(rows: List[Dict], path: str) -> None:
    import csv
    from pathlib import Path

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)

    with target.open("w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=PRODUCTS_CSV_COLUMNS)
        writer.writeheader()
        for chunk_start in range(0, len(rows), 40000):
            chunk = rows[chunk_start : chunk_start + 40000]
            writer.writerows(chunk)


def format_change_time(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    value = str(raw).strip()
    if not value:
        return None
    if "." in value:
        value = value.split(".", 1)[0]
    value = value.replace("T", " ")
    return value[:19]


def fetch_ad_list_sellers(cursor: mysql.connector.cursor.MySQLCursorDict) -> List[Dict]:
    cursor.execute(
        """
        SELECT
            s.seller_id,
            s.wb_api_key,
            s.wb_api_brand,
            su.ad_list_status
        FROM WB_sellers AS s
        JOIN WB_sellers_updates AS su
            ON su.seller_id = s.seller_id
        WHERE su.in_workrnp = 1
        """
    )
    rows = cursor.fetchall()
    result: List[Dict] = []
    for row in rows:
        status = row.get("ad_list_status")
        if isinstance(status, str):
            try:
                row["ad_list_status"] = json.loads(status)
            except json.JSONDecodeError:
                row["ad_list_status"] = None
        result.append(row)
    return result


def compute_ad_list_priority(
    sellers: Iterable[Dict],
) -> Tuple[List[Dict], List[Dict], str]:
    processed: List[Dict] = []
    per_seller: Dict[str, Dict] = {}

    for idx, raw in enumerate(sellers):
        seller_id = str(raw.get("seller_id") or "").strip()
        if not seller_id:
            continue

        ad_status = raw.get("ad_list_status") or None
        brand = (raw.get("wb_api_brand") or seller_id).strip()

        status_cat = "new" if not ad_status else "existing"
        now_time = ad_status.get("nowTime") if ad_status else None

        bucket = per_seller.get(seller_id)
        if bucket is None:
            bucket = {
                "seller_id": seller_id,
                "brand": brand or seller_id,
                "status_cat": status_cat,
                "idx": idx,
                "ad_list_status": ad_status,
                "now_time": now_time,
                "cooldown_blocked": False,
            }
            per_seller[seller_id] = bucket
        else:
            if bucket["status_cat"] != "new" and status_cat == "new":
                bucket["status_cat"] = status_cat
            if not bucket.get("brand"):
                bucket["brand"] = brand
            if not bucket.get("now_time") and now_time:
                bucket["now_time"] = now_time

        processed.append(
            {
                "seller_id": seller_id,
                "brand": brand or seller_id,
                "status_cat": status_cat,
                "idx": idx,
                "wb_api_key": raw.get("wb_api_key"),
                "ad_list_status": ad_status,
                "now_time": now_time,
                "cooldown_blocked": False,
            }
        )

    all_sellers = list(per_seller.values())

    new_group = [s for s in all_sellers if s["status_cat"] == "new"]
    existing_group = [s for s in all_sellers if s["status_cat"] != "new"]

    allow_set = set()

    if new_group:
        allow_set.update(s["seller_id"] for s in new_group)
    else:
        for s in existing_group:
            status_dict = s.get("ad_list_status") or {}
            now_dt = parse_msk_datetime(status_dict.get("nowTime") or s.get("now_time"))
            if now_dt is None:
                allow_set.add(s["seller_id"])
            else:
                age_min = minutes_since_msk(now_dt)
                if age_min >= ADS_COOLDOWN_MINUTES:
                    allow_set.add(s["seller_id"])
                else:
                    s["cooldown_blocked"] = True

    lines = []
    for s in sorted(all_sellers, key=lambda item: item["brand"].lower()):
        allowed = s["seller_id"] in allow_set
        mark = "✅" if allowed else "✖️"
        status_dict = s.get("ad_list_status") or {}
        now_dt = parse_msk_datetime(status_dict.get("nowTime") or s.get("now_time"))
        if s["status_cat"] == "new":
            minutes_text = "new"
        elif now_dt is not None:
            age_min = max(0, int(minutes_since_msk(now_dt)))
            minutes_text = f"{age_min}min"
        else:
            minutes_text = "--"
        lines.append(f"{minutes_text} | {s['brand']} {mark}")

    summary_text = "\n".join(lines)

    allowed_items: List[Dict] = []
    for item in processed:
        if item["seller_id"] in allow_set:
            enriched = dict(item)
            enriched["text"] = summary_text
            allowed_items.append(enriched)

    return allowed_items, all_sellers, summary_text


def build_ad_list_selection_message(summary_text: str) -> str:
    escaped = html.escape(summary_text)
    return (
        "<b>04 WB API</b> | Ad List\n"
        "<blockquote>Подготовка выгрузки рекламных кампаний.\n"
        f"<code>{escaped}</code></blockquote>"
    )


def fetch_ad_list_for_seller(item: Dict) -> Dict:
    headers = {
        "user-agent": WB_USER_AGENT,
        "Authorization": f"Bearer {(item.get('wb_api_key') or '').strip()}",
    }
    response = requests.get(WB_AD_LIST_URL, headers=headers, timeout=30)
    if response.status_code != 200:
        raise RuntimeError(
            f"WB Ad List API error for {item['seller_id']}: {response.status_code} {response.text}"
        )

    payload = response.json()
    adverts_payload = payload.get("adverts") or []
    rows: List[Dict] = []
    total_all = 0
    active_cnt = 0
    paused_cnt = 0

    if adverts_payload:
        for block in adverts_payload:
            advert_list = block.get("advert_list") or block.get("advertList") or []
            ad_type = block.get("type")
            try:
                block_status = int(block.get("status"))
            except (TypeError, ValueError):
                block_status = None
            block_count = int(block.get("count") or len(advert_list) or 0)
            total_all += block_count

            if block_status == 9:
                active_cnt += block_count
            elif block_status == 11:
                paused_cnt += block_count

            if not isinstance(advert_list, list):
                continue
            for advert in advert_list:
                advert_id = advert.get("advertId")
                change_time = format_change_time(advert.get("changeTime"))
                if advert_id is None:
                    continue
                rows.append(
                    {
                        "seller_id": item["seller_id"],
                        "advertId": advert_id,
                        "changeTime": change_time,
                        "type": ad_type,
                        "status": block_status,
                        "advert_seller_key": f"{advert_id}_{item['seller_id']}",
                    }
                )

    counts = {
        "all": total_all,
        "active": active_cnt,
        "paused": paused_cnt,
    }

    return {
        "seller_id": item["seller_id"],
        "brand": item.get("brand") or item["seller_id"],
        "counts": counts,
        "rows": rows,
        "ad_list_status": item.get("ad_list_status"),
    }


def prepare_ad_stats_requests(sellers: Iterable[Dict]) -> Tuple[List[Dict], List[Dict], str]:
    today_str = ymd(msk_now())
    default_from = ymd_minus_days(AD_STATS_DEFAULT_LOOKBACK_DAYS)
    threshold_180 = ymd_minus_days(AD_STATS_LOOKBACK_DAYS)

    processed: List[Dict] = []

    for idx, raw in enumerate(sellers):
        seller_id = str(raw.get("seller_id") or "").strip()
        if not seller_id:
            continue
        brand = (raw.get("wb_api_brand") or seller_id).strip()
        token = (raw.get("wb_api_key") or "").strip()

        campaigns_raw = raw.get("campaigns") or []
        campaign_ids: Set[int] = set()
        excluded_count = 0
        for campaign in campaigns_raw:
            advert_id = campaign.get("advertId")
            status_value = campaign.get("status")
            try:
                advert_id_int = int(advert_id)
            except (TypeError, ValueError):
                continue
            try:
                status_int = int(status_value)
            except (TypeError, ValueError):
                excluded_count += 1
                continue
            if status_int in AD_STATS_ALLOWED_STATUSES:
                campaign_ids.add(advert_id_int)
            else:
                excluded_count += 1

        status_obj = raw.get("ad_stats_status") if isinstance(raw.get("ad_stats_status"), dict) else None
        now_time = status_obj.get("nowTime") if status_obj else None
        status_value = (status_obj or {}).get("status")
        if status_obj is None:
            status_cat = "new"
        elif status_value == "left":
            status_cat = "left"
        elif status_value == "right":
            status_cat = "right"
        else:
            status_cat = "other"

        right_boundary = first_ymd((status_obj or {}).get("rightBoundary"))
        max_begin_date = first_ymd((status_obj or {}).get("maxBeginDate"))

        if status_cat == "new":
            intervals = chunk_between(default_from, today_str)
        elif status_cat == "left":
            if max_begin_date and max_begin_date > threshold_180:
                intervals = chunk_between(threshold_180, today_str)
            elif max_begin_date and max_begin_date <= threshold_180:
                if right_boundary:
                    start = sub_days_from_ymd(right_boundary, AD_STATS_MAX_INTERVAL_DAYS - 1) or default_from
                    intervals = chunk_between(start, right_boundary)
                else:
                    intervals = chunk_between(default_from, today_str)
            else:
                intervals = chunk_between(default_from, today_str)
        elif status_cat == "right":
            if right_boundary:
                start = sub_days_from_ymd(right_boundary, 7) or default_from
                intervals = chunk_between(start, today_str)
            else:
                intervals = chunk_between(default_from, today_str)
        else:
            intervals = chunk_between(default_from, today_str)

        processed.append(
            {
                "seller_id": seller_id,
                "brand": brand,
                "token": token,
                "idx": idx,
                "status_cat": status_cat,
                "now_time": now_time,
                "campaign_ids": sorted(campaign_ids),
                "campaign_ids_list": list(sorted(campaign_ids)),  # для передачи в update_ad_stats_status
                "excluded_count": excluded_count,
                "intervals": intervals,
                "ad_stats_status": status_obj,
                "ad_stats_status_raw": raw.get("ad_stats_status_raw"),
            }
        )

    def parse_now_time(value: Optional[str]) -> float:
        dt = parse_msk_datetime(value)
        if dt is None:
            return float("inf")
        return dt.timestamp()

    def ad_stats_right_cooldown_minutes(status_obj: Optional[Dict]) -> float:
        if not status_obj:
            return 0.0
        now_time_str = status_obj.get("nowTime")
        dt = parse_msk_datetime(now_time_str)
        if dt is None:
            return 0.0
        age_min = minutes_since_msk(dt)
        remaining = AD_STATS_RIGHT_COOLDOWN_MINUTES - age_min
        return max(0.0, remaining)

    new_group = [s for s in processed if s["status_cat"] == "new"]
    left_group = [s for s in processed if s["status_cat"] == "left"]
    right_group = [s for s in processed if s["status_cat"] == "right"]

    allow_set: Set[str] = set()
    if new_group:
        allow_set.update(s["seller_id"] for s in new_group)
    if left_group:
        left_group_sorted = sorted(left_group, key=lambda s: (parse_now_time(s["now_time"]), s["idx"]))
        if left_group_sorted:
            allow_set.add(left_group_sorted[0]["seller_id"])
    if right_group:
        for s in right_group:
            cooldown_remaining = ad_stats_right_cooldown_minutes(s.get("ad_stats_status"))
            if cooldown_remaining <= 0:
                allow_set.add(s["seller_id"])

    summary_lines = []
    for s in sorted(processed, key=lambda item: item["brand"].lower()):
        intervals = s["intervals"]
        
        # Вычисляем минуты с последнего прогона
        minutes_ago = ""
        if s.get("now_time"):
            dt = parse_msk_datetime(s["now_time"])
            if dt:
                minutes_ago_int = int(minutes_since_msk(dt))
                minutes_ago = f"{minutes_ago_int}min | "
        
        # Вычисляем мин/макс границы дат из всех интервалов
        min_begin_date = None
        max_end_date = None
        if intervals:
            for interval in intervals:
                begin_date = first_ymd(interval.get("beginDate"))
                end_date = first_ymd(interval.get("endDate"))
                if begin_date:
                    min_begin_date = min_begin_date if min_begin_date and min_begin_date < begin_date else begin_date
                if end_date:
                    max_end_date = max_end_date if max_end_date and max_end_date > end_date else end_date
        
        begin_short = format_date_short(min_begin_date) if min_begin_date else "-"
        end_short = format_date_short(max_end_date) if max_end_date else "-"
        mark = "✅" if s["seller_id"] in allow_set else "✖️"
        summary_lines.append(
            f"{minutes_ago}{s['status_cat']} | {begin_short} - {end_short} | "
            f"{len(s['campaign_ids'])} | {s['brand']} {mark}"
        )
    summary_text = "\n".join(summary_lines)

    allowed_effective = [
        s for s in processed if s["seller_id"] in allow_set and s["campaign_ids"]
    ]

    requests: List[Dict] = []
    per_seller_emit: Dict[str, int] = defaultdict(int)
    per_seller_base: Dict[str, float] = {}
    total_sellers = len(allowed_effective)

    for seller_index, s in enumerate(sorted(allowed_effective, key=lambda item: item["brand"].lower()), start=1):
        campaign_ids = s["campaign_ids"]
        campaign_chunks = [
            campaign_ids[i : i + AD_STATS_MAX_CAMPAIGNS_PER_REQUEST]
            for i in range(0, len(campaign_ids), AD_STATS_MAX_CAMPAIGNS_PER_REQUEST)
        ]
        for chunk_index, chunk in enumerate(campaign_chunks, start=1):
            for interval in s["intervals"]:
                emitted = per_seller_emit[s["seller_id"]]
                base_ts = per_seller_base.setdefault(s["seller_id"], msk_now().timestamp())
                offset = (
                    (emitted // AD_STATS_RATE_LIMIT_REQUESTS_PER_MINUTE) * 60
                    + (emitted % AD_STATS_RATE_LIMIT_REQUESTS_PER_MINUTE) * AD_STATS_RATE_INTERVAL_SECONDS
                )
                ready_at_ts = base_ts + offset
                ready_at_dt = datetime.fromtimestamp(ready_at_ts, tz=MSK_OFFSET)
                ready_at_iso = ready_at_dt.strftime("%Y-%m-%d %H:%M:%S")
                per_seller_emit[s["seller_id"]] = emitted + 1

                requests.append(
                    {
                        "seller_id": s["seller_id"],
                        "brand": s["brand"],
                        "token": s["token"],
                        "campaign_ids": chunk,
                        "campaign_ids_csv": ",".join(str(cid) for cid in chunk),
                        "chunk_index": chunk_index,
                        "chunk_total": len(campaign_chunks),
                        "interval": interval,
                        "excluded_count": s["excluded_count"],
                        "total_campaigns": len(campaign_ids),
                        "ready_at_ts": ready_at_ts,
                        "ready_at_iso": ready_at_iso,
                        "request_index": emitted + 1,
                        "seller_index": seller_index,
                        "sellers_total": total_sellers,
                        "ad_stats_status": s["ad_stats_status"],
                        "ad_stats_status_raw": s["ad_stats_status_raw"],
                    }
                )

    return requests, processed, summary_text


def fetch_total_nmid_sellers(cursor: mysql.connector.cursor.MySQLCursorDict) -> List[Dict]:
    cursor.execute(
        """
        SELECT
            s.seller_id,
            s.wb_api_brand,
            su.total_nmid_status
        FROM WB_sellers AS s
        JOIN WB_sellers_updates AS su
            ON su.seller_id = s.seller_id
        WHERE su.in_workrnp = 1
        """
    )
    rows = cursor.fetchall()
    result: List[Dict] = []
    for row in rows:
        seller_id = str(row.get("seller_id") or "").strip()
        if not seller_id:
            continue
        status = parse_json_field(row.get("total_nmid_status"))
        result.append(
            {
                "seller_id": seller_id,
                "brand": str(row.get("wb_api_brand") or seller_id).strip(),
                "total_nmid_status": status if isinstance(status, dict) else None,
            }
        )
    return result


def _total_nmid_cooldown_minutes(status_obj: Optional[Dict]) -> float:
    if not status_obj:
        return 0.0
    now_time_str = status_obj.get("nowTime")
    dt = parse_msk_datetime(now_time_str)
    if dt is None:
        return 0.0
    age_min = minutes_since_msk(dt)
    remaining = TOTAL_NMID_RIGHT_COOLDOWN_MINUTES - age_min
    return max(0.0, remaining)


def prepare_total_nmid_requests(
    sellers: Iterable[Dict],
) -> Tuple[List[Dict], List[Dict], str]:
    processed: List[Dict] = []

    for idx, raw in enumerate(sellers):
        seller_id = str(raw.get("seller_id") or "").strip()
        if not seller_id:
            continue
        status_obj = raw.get("total_nmid_status")
        status_cat = "new"
        now_time = None
        if isinstance(status_obj, dict):
            status_cat_value = (status_obj.get("status") or "").strip().lower()
            now_time = status_obj.get("nowTime")
            if status_cat_value == "right":
                status_cat = "right"
            else:
                status_cat = "new"
        processed.append(
            {
                "seller_id": seller_id,
                "brand": raw.get("brand") or seller_id,
                "idx": idx,
                "total_nmid_status": status_obj,
                "status_cat": status_cat,
                "now_time": now_time,
            }
        )

    new_group = [s for s in processed if s["status_cat"] == "new"]
    right_group = [s for s in processed if s["status_cat"] == "right"]

    allow_set: Set[str] = set()
    if new_group:
        allow_set.update(s["seller_id"] for s in new_group)
    else:
        for s in right_group:
            cooldown_remaining = _total_nmid_cooldown_minutes(s.get("total_nmid_status"))
            if cooldown_remaining <= 0:
                allow_set.add(s["seller_id"])

    summary_lines = []
    for s in sorted(processed, key=lambda item: item["brand"].lower()):
        allowed = s["seller_id"] in allow_set
        mark = "✅" if allowed else "✖️"
        status_obj = s.get("total_nmid_status") or {}
        now_dt = parse_msk_datetime(status_obj.get("nowTime"))
        if s["status_cat"] == "new":
            minutes_text = "new"
        elif now_dt is not None:
            age_min = max(0, int(minutes_since_msk(now_dt)))
            minutes_text = f"{age_min}min"
        else:
            minutes_text = "--"
        summary_lines.append(f"{minutes_text} | {s['brand']} {mark}")
    summary_text = "\n".join(summary_lines)

    allowed = [s for s in processed if s["seller_id"] in allow_set]
    return allowed, processed, summary_text


def prepare_ad_expenses_requests(sellers: Iterable[Dict]) -> Tuple[List[Dict], List[Dict], str]:
    today_str = ymd(msk_now())
    default_from = ymd_minus_days(AD_EXPENSES_DEFAULT_LOOKBACK_DAYS)
    threshold_180 = ymd_minus_days(AD_EXPENSES_LOOKBACK_DAYS)

    processed: List[Dict] = []

    for idx, raw in enumerate(sellers):
        seller_id = str(raw.get("seller_id") or "").strip()
        if not seller_id:
            continue
        brand = (raw.get("wb_api_brand") or seller_id).strip()
        token = (raw.get("wb_api_key") or "").strip()
        campaigns_cnt = int(raw.get("campaigns_cnt", 0) or 0)

        # Читаем статус: приоритет ad_expenses_status, fallback на ad_stats_status
        status_obj = raw.get("ad_expenses_status")
        if not status_obj and raw.get("ad_stats_status"):
            status_obj = raw.get("ad_stats_status")
        
        # Если campaigns_cnt = 0 и нет статуса, сразу ставим right
        if campaigns_cnt == 0 and not status_obj:
            status_obj = {
                "status": "right",
                "nowTime": msk_now().strftime("%Y-%m-%d %H:%M:%S"),
                "lastTotalRow": 0,
                "leftBoundary": "",
                "maxBeginDate": "",
                "lastBeginDate": "",
                "rightBoundary": "",
            }

        now_time = status_obj.get("nowTime") if isinstance(status_obj, dict) else None
        status_value = (status_obj or {}).get("status") if isinstance(status_obj, dict) else None
        
        if status_obj is None:
            status_cat = "new"
        elif status_value == "left":
            status_cat = "left"
        elif status_value == "right":
            status_cat = "right"
        else:
            status_cat = "other"

        right_boundary = first_ymd((status_obj or {}).get("rightBoundary")) if isinstance(status_obj, dict) else None
        max_begin_date = first_ymd((status_obj or {}).get("maxBeginDate")) if isinstance(status_obj, dict) else None

        if status_cat == "new":
            intervals = chunk_between(default_from, today_str, AD_EXPENSES_MAX_INTERVAL_DAYS)
        elif status_cat == "left":
            if max_begin_date and max_begin_date > threshold_180:
                intervals = chunk_between(threshold_180, today_str, AD_EXPENSES_MAX_INTERVAL_DAYS)
            elif max_begin_date and max_begin_date <= threshold_180:
                if right_boundary:
                    start = sub_days_from_ymd(right_boundary, AD_EXPENSES_MAX_INTERVAL_DAYS - 1) or default_from
                    intervals = chunk_between(start, right_boundary, AD_EXPENSES_MAX_INTERVAL_DAYS)
                else:
                    intervals = chunk_between(default_from, today_str, AD_EXPENSES_MAX_INTERVAL_DAYS)
            else:
                intervals = chunk_between(default_from, today_str, AD_EXPENSES_MAX_INTERVAL_DAYS)
        elif status_cat == "right":
            if right_boundary:
                start = sub_days_from_ymd(right_boundary, 7) or default_from
                intervals = chunk_between(start, today_str, AD_EXPENSES_MAX_INTERVAL_DAYS)
            else:
                intervals = chunk_between(default_from, today_str, AD_EXPENSES_MAX_INTERVAL_DAYS)
        else:
            intervals = chunk_between(default_from, today_str, AD_EXPENSES_MAX_INTERVAL_DAYS)

        processed.append(
            {
                "seller_id": seller_id,
                "brand": brand,
                "token": token,
                "idx": idx,
                "status_cat": status_cat,
                "now_time": now_time,
                "intervals": intervals,
                "ad_expenses_status": status_obj,
                "ad_expenses_status_raw": raw.get("ad_expenses_status_raw"),
                "campaigns_cnt": campaigns_cnt,
            }
        )

    def parse_now_time(value: Optional[str]) -> float:
        dt = parse_msk_datetime(value)
        if dt is None:
            return float("inf")
        return dt.timestamp()

    def ad_expenses_right_cooldown_minutes(status_obj: Optional[Dict]) -> float:
        if not status_obj:
            return 0.0
        now_time_str = status_obj.get("nowTime")
        dt = parse_msk_datetime(now_time_str)
        if dt is None:
            return 0.0
        age_min = minutes_since_msk(dt)
        remaining = AD_EXPENSES_RIGHT_COOLDOWN_MINUTES - age_min
        return max(0.0, remaining)

    new_group = [s for s in processed if s["status_cat"] == "new"]
    left_group = [s for s in processed if s["status_cat"] == "left"]
    right_group = [s for s in processed if s["status_cat"] == "right"]

    allow_set: Set[str] = set()
    if new_group:
        allow_set.update(s["seller_id"] for s in new_group)
    if left_group:
        left_group_sorted = sorted(left_group, key=lambda s: (parse_now_time(s["now_time"]), s["idx"]))
        if left_group_sorted:
            allow_set.add(left_group_sorted[0]["seller_id"])
    if right_group:
        for s in right_group:
            cooldown_remaining = ad_expenses_right_cooldown_minutes(s.get("ad_expenses_status"))
            if cooldown_remaining <= 0:
                allow_set.add(s["seller_id"])

    summary_lines = []
    for s in sorted(processed, key=lambda item: item["brand"].lower()):
        intervals = s["intervals"]
        
        # Вычисляем минуты с последнего прогона
        minutes_ago = ""
        if s.get("now_time"):
            dt = parse_msk_datetime(s["now_time"])
            if dt:
                minutes_ago_int = int(minutes_since_msk(dt))
                minutes_ago = f"{minutes_ago_int}min | "
        
        # Вычисляем мин/макс границы дат из всех интервалов
        min_begin_date = None
        max_end_date = None
        if intervals:
            for interval in intervals:
                begin_date = first_ymd(interval.get("beginDate"))
                end_date = first_ymd(interval.get("endDate"))
                if begin_date:
                    min_begin_date = min_begin_date if min_begin_date and min_begin_date < begin_date else begin_date
                if end_date:
                    max_end_date = max_end_date if max_end_date and max_end_date > end_date else end_date
        
        begin_short = format_date_short(min_begin_date) if min_begin_date else "-"
        end_short = format_date_short(max_end_date) if max_end_date else "-"
        mark = "✅" if s["seller_id"] in allow_set else "✖️"
        campaigns_cnt = s.get("campaigns_cnt", 0)
        summary_lines.append(
            f"{minutes_ago}{s['status_cat']} | {begin_short} - {end_short} | {campaigns_cnt} | {s['brand']} {mark}"
        )
    summary_text = "\n".join(summary_lines)

    allowed_effective = [
        s for s in processed if s["seller_id"] in allow_set
    ]

    requests: List[Dict] = []
    per_seller_emit: Dict[str, int] = defaultdict(int)
    total_sellers = len(allowed_effective)

    for seller_index, s in enumerate(sorted(allowed_effective, key=lambda item: item["brand"].lower()), start=1):
        intervals = s["intervals"]
        for interval_index, interval in enumerate(intervals, start=1):
            emitted = per_seller_emit[s["seller_id"]]
            delay_flag = 1 if emitted > 0 else 0
            per_seller_emit[s["seller_id"]] = emitted + 1

            requests.append(
                {
                    "seller_id": s["seller_id"],
                    "brand": s["brand"],
                    "token": s["token"],
                    "interval": interval,
                    "beginDate": interval["beginDate"],
                    "endDate": interval["endDate"],
                    "delay": delay_flag,
                    "interval_index": interval_index,
                    "interval_total": len(intervals),
                    "seller_index": seller_index,
                    "sellers_total": total_sellers,
                    "ad_expenses_status": s.get("ad_expenses_status"),
                }
            )

    return requests, processed, summary_text


def fetch_stocks_sellers(cursor: mysql.connector.cursor.MySQLCursorDict) -> List[Dict]:
    cursor.execute(
        """
        SELECT
            s.seller_id,
            s.wb_api_key,
            s.wb_api_brand,
            su.stock_status
        FROM WB_sellers AS s
        JOIN WB_sellers_updates AS su
            ON su.seller_id = s.seller_id
        WHERE su.in_workrnp = 1
        """
    )
    rows = cursor.fetchall()
    result: List[Dict] = []
    for row in rows:
        seller_id = str(row.get("seller_id") or "").strip()
        if not seller_id:
            continue
        # Парсим JSON поле stock_status
        status = row.get("stock_status")
        stock_status = None
        if status:
            try:
                if isinstance(status, str):
                    stock_status = json.loads(status)
                elif isinstance(status, dict):
                    stock_status = status
            except (json.JSONDecodeError, TypeError):
                stock_status = None
        result.append(
            {
                "seller_id": seller_id,
                "wb_api_key": str(row.get("wb_api_key") or "").strip(),
                "wb_api_brand": str(row.get("wb_api_brand") or seller_id).strip(),
                "stock_status": stock_status,
            }
        )
    return result


def fetch_report_detail_sellers(cursor: mysql.connector.cursor.MySQLCursorDict) -> List[Dict]:
    cursor.execute(
        """
        SELECT
            s.seller_id,
            COALESCE(s.wb_api_brand, s.seller_id) AS brand
        FROM WB_sellers AS s
        JOIN WB_sellers_updates AS su
            ON su.seller_id = s.seller_id
        WHERE su.in_workrnp = 1
        """
    )
    rows = cursor.fetchall()
    result: List[Dict] = []
    for row in rows:
        seller_id = str(row.get("seller_id") or "").strip()
        if not seller_id:
            continue
        result.append(
            {
                "seller_id": seller_id,
                "brand": str(row.get("brand") or seller_id).strip(),
            }
        )
    return result


REPORT_DETAIL_INSERT_ORDERS_SQL = """
INSERT INTO GS_RNP_ReportDetail (
    srid_seller_sale_key,
    seller_id,
    Date_order,
    Date_cancel,
    Warehouse,
    Warehouse_type,
    Country,
    Okrug,
    Region,
    nmId,
    supplierArticle,
    Size,
    BC,
    Brand,
    Category,
    Subject,
    SPP,
    TotalPrice,
    Discount,
    priceWithDisc,
    finishedPrice,
    gNumber,
    srid,
    orderType,
    status
)
SELECT
    CONCAT(srid, '_', seller_id) AS srid_seller_sale_key,
    seller_id,
    `date` AS Date_order,
    CASE WHEN isCancel = 1 THEN cancelDate ELSE NULL END AS Date_cancel,
    warehouseName       AS Warehouse,
    warehouseType       AS Warehouse_type,
    countryName         AS Country,
    oblastOkrugName     AS Okrug,
    regionName          AS Region,
    nmId,
    supplierArticle,
    techSize            AS Size,
    barcode             AS BC,
    brand               AS Brand,
    category            AS Category,
    subject             AS Subject,
    spp                 AS SPP,
    totalPrice          AS TotalPrice,
    discountPercent     AS Discount,
    priceWithDisc       AS priceWithDisc,
    finishedPrice       AS finishedPrice,
    gNumber,
    srid,
    orderType,
    CASE WHEN isCancel = 1 THEN 'Отмена' ELSE 'Доставка' END AS status
FROM WB_orders
WHERE seller_id = %s
ON DUPLICATE KEY UPDATE
  seller_id       = VALUES(seller_id),
  Date_order      = VALUES(Date_order),
  Date_cancel     = VALUES(Date_cancel),
  Warehouse       = VALUES(Warehouse),
  Warehouse_type  = VALUES(Warehouse_type),
  Country         = VALUES(Country),
  Okrug           = VALUES(Okrug),
  Region          = VALUES(Region),
  nmId            = VALUES(nmId),
  supplierArticle = VALUES(supplierArticle),
  Size            = VALUES(Size),
  BC              = VALUES(BC),
  Brand           = VALUES(Brand),
  Category        = VALUES(Category),
  Subject         = VALUES(Subject),
  SPP             = VALUES(SPP),
  TotalPrice      = VALUES(TotalPrice),
  Discount        = VALUES(Discount),
  priceWithDisc   = VALUES(priceWithDisc),
  finishedPrice   = VALUES(finishedPrice),
  gNumber         = VALUES(gNumber),
  srid            = VALUES(srid),
  orderType       = VALUES(orderType),
  status          = VALUES(status)
"""


REPORT_DETAIL_INSERT_SALES_SQL = """
INSERT INTO GS_RNP_ReportDetail (
    srid_seller_sale_key,
    seller_id,
    date_sales,
    date_return,
    finishedPriceFact,
    paymentSaleAmount,
    forPay,
    ComWBRub,
    ComWBPercent,
    Warehouse,
    Warehouse_type,
    Country,
    Okrug,
    Region,
    nmId,
    supplierArticle,
    Size,
    BC,
    Brand,
    Category,
    Subject,
    SPP,
    TotalPrice,
    Discount,
    gNumber,
    srid,
    orderType,
    status,
    priceWithDisc,
    finishedPrice
)
SELECT
  CONCAT(w.srid, '_', w.seller_id) AS srid_seller_sale_key,
  w.seller_id,
  ls.lastSaleDate AS date_sales,
  lr.lastReturnDate AS date_return,
  ABS(w.finishedPrice)     AS finishedPriceFact,
  ABS(w.paymentSaleAmount) AS paymentSaleAmount,
  ABS(w.forPay)            AS forPay,
  ROUND(ABS(w.priceWithDisc) - ABS(w.forPay), 2) AS ComWBRub,
  CASE
    WHEN ABS(w.priceWithDisc) != 0
    THEN ROUND(((ABS(w.priceWithDisc) - ABS(w.forPay)) * 100 / ABS(w.priceWithDisc)), 2)
    ELSE 0
  END AS ComWBPercent,
  w.warehouseName   AS Warehouse,
  w.warehouseType   AS Warehouse_type,
  w.countryName     AS Country,
  w.oblastOkrugName AS Okrug,
  w.regionName      AS Region,
  w.nmId,
  w.supplierArticle,
  w.techSize        AS Size,
  w.barcode         AS BC,
  w.brand           AS Brand,
  w.category        AS Category,
  w.subject         AS Subject,
  w.spp             AS SPP,
  ABS(w.totalPrice)     AS TotalPrice,
  w.discountPercent     AS Discount,
  w.gNumber,
  w.srid,
  w.orderType,
  CASE
    WHEN lr.lastReturnDate IS NOT NULL THEN 'Возврат'
    WHEN ls.lastSaleDate   IS NOT NULL THEN 'Выкуп'
    ELSE NULL
  END AS status,
  ABS(w.priceWithDisc)   AS priceWithDisc,
  ABS(w.finishedPrice)    AS finishedPrice
FROM
  (
    SELECT
      srid,
      seller_id,
      MAX(`date`) AS lastEventDate
    FROM WB_sales
    GROUP BY srid, seller_id
  ) le
  JOIN WB_sales w
    ON  w.srid      = le.srid
    AND w.seller_id = le.seller_id
    AND w.`date`    = le.lastEventDate
  LEFT JOIN (
    SELECT
      srid,
      seller_id,
      MAX(`date`) AS lastReturnDate
    FROM WB_sales
    WHERE saleID LIKE 'R%'
    GROUP BY srid, seller_id
  ) lr
    ON lr.srid      = le.srid
    AND lr.seller_id = le.seller_id
  LEFT JOIN (
    SELECT
      srid,
      seller_id,
      MAX(`date`) AS lastSaleDate
    FROM WB_sales
    WHERE saleID LIKE 'S%'
    GROUP BY srid, seller_id
  ) ls
    ON ls.srid      = le.srid
    AND ls.seller_id = le.seller_id
WHERE w.seller_id = %s
ON DUPLICATE KEY UPDATE
  seller_id         = VALUES(seller_id),
  date_sales        = VALUES(date_sales),
  date_return       = VALUES(date_return),
  finishedPriceFact = VALUES(finishedPriceFact),
  paymentSaleAmount = VALUES(paymentSaleAmount),
  forPay            = VALUES(forPay),
  ComWBRub          = VALUES(ComWBRub),
  ComWBPercent      = VALUES(ComWBPercent),
  Warehouse         = VALUES(Warehouse),
  Warehouse_type    = VALUES(Warehouse_type),
  Country           = VALUES(Country),
  Okrug             = VALUES(Okrug),
  Region            = VALUES(Region),
  nmId              = VALUES(nmId),
  supplierArticle   = VALUES(supplierArticle),
  Size              = VALUES(Size),
  BC                = VALUES(BC),
  Brand             = VALUES(Brand),
  Category          = VALUES(Category),
  Subject           = VALUES(Subject),
  SPP               = VALUES(SPP),
  TotalPrice        = VALUES(TotalPrice),
  Discount          = VALUES(Discount),
  gNumber           = VALUES(gNumber),
  srid              = VALUES(srid),
  orderType         = VALUES(orderType),
  status            = VALUES(status),
  priceWithDisc     = VALUES(priceWithDisc),
  finishedPrice     = VALUES(finishedPrice)
"""


REPORT_DETAIL_UPDATE_PURPRICE_SQL = """
UPDATE GS_RNP_ReportDetail r
JOIN GS_RNP_PurPrice p
   ON p.Barcode = r.BC
  AND p.seller_id = r.seller_id
  AND p.Date = (
       SELECT MAX(p2.Date)
       FROM GS_RNP_PurPrice p2
       WHERE p2.Barcode = r.BC
         AND p2.seller_id = r.seller_id
         AND p2.Date <= COALESCE(r.Date_order, r.date_sales, r.date_return)
  )
SET
  r.Pur_price              = p.Pur_price,
  r.Logistics_toclient_cost= p.Logistics_cost,
  r.Pur_date               = p.Date
WHERE (
       r.Pur_price IS NULL
       OR r.Pur_date IS NULL
       OR r.Pur_date < p.Date
       OR r.Pur_price <> p.Pur_price
      )
"""


REPORT_DETAIL_UPDATE_LOGISTICS_SQL = """
UPDATE GS_RNP_ReportDetail
SET Logistics_return_cost = CASE
  WHEN status IN ('Отмена', 'Возврат') THEN 50
  ELSE 0
END
"""


TOTAL_NMID_PRODUCTS_SQL = """
SELECT
  p.seller_id,
  p.nmid,
  ANY_VALUE(p.vendorCode)        AS vendorCode,
  ANY_VALUE(p.subjectName)       AS subjectName,
  ANY_VALUE(p.title)             AS title,
  ANY_VALUE(p.photo)             AS photo,
  ANY_VALUE(s.wb_api_brand)      AS wb_api_brand,
  MAX(su.orders_status)          AS orders_status,
  MAX(su.sales_status)           AS sales_status,
  MAX(su.total_nmid_status)      AS total_nmid_status,
  MAX(su.paid_storage_status)    AS paid_storage_status,
  MAX(su.ad_stats_status)        AS ad_stats_status
FROM WB_products p
LEFT JOIN WB_sellers         s  ON s.seller_id  = p.seller_id
LEFT JOIN WB_sellers_updates su ON su.seller_id = p.seller_id
WHERE p.seller_id = %s
GROUP BY p.seller_id, p.nmid
ORDER BY p.nmid
"""


TOTAL_NMID_FETCH_SQL = """
SELECT
    p.seller_id,
    p.nmID,
    p.vendorCode,
    p.subjectName,
    p.title,
    p.photo,
    COALESCE(s.wb_api_brand, '') AS wb_api_brand,
    d.BC,
    d.Size,
    d.supplierArticle,
    d.Subject,
    d.id,
    d.status,
    d.srid_seller_sale_key,
    d.Date_order,
    d.Date_cancel,
    d.date_sales,
    d.date_return,
    d.Warehouse,
    d.Warehouse_type,
    d.Country,
    d.Okrug,
    d.Region,
    d.nmId                AS nmId_sale,
    d.SPP,
    d.TotalPrice,
    d.Discount,
    d.priceWithDisc,
    d.finishedPrice,
    d.gNumber,
    d.srid,
    d.orderType,
    d.finishedPriceFact,
    d.paymentSaleAmount,
    d.forPay,
    d.ComWBRub,
    d.ComWBPercent,
    d.Pur_price,
    d.Logistics_toclient_cost,
    d.Logistics_return_cost,
    d.Pur_date
FROM (
    SELECT
        seller_id,
        nmID,
        MIN(vendorCode)   AS vendorCode,
        MIN(subjectName)  AS subjectName,
        MIN(title)        AS title,
        MIN(photo)        AS photo
    FROM WB_products
    WHERE seller_id = %s
      AND nmID      = %s
    GROUP BY seller_id, nmID
) AS p
LEFT JOIN WB_sellers AS s
       ON s.seller_id = p.seller_id
LEFT JOIN GS_RNP_ReportDetail AS d
       ON  d.seller_id = p.seller_id
       AND d.nmId      = p.nmID
       AND d.Date_order >= %s
       AND d.Date_order < %s
"""

TOTAL_NMID_FETCH_BY_SELLER_SQL = """
SELECT
    p.seller_id,
    p.nmID,
    p.vendorCode,
    p.subjectName,
    p.title,
    p.photo,
    COALESCE(s.wb_api_brand, '') AS wb_api_brand,
    d.BC,
    d.Size,
    d.supplierArticle,
    d.Subject,
    d.id,
    d.status,
    d.srid_seller_sale_key,
    d.Date_order,
    d.Date_cancel,
    d.date_sales,
    d.date_return,
    d.Warehouse,
    d.Warehouse_type,
    d.Country,
    d.Okrug,
    d.Region,
    d.nmId                AS nmId_sale,
    d.SPP,
    d.TotalPrice,
    d.Discount,
    d.priceWithDisc,
    d.finishedPrice,
    d.gNumber,
    d.srid,
    d.orderType,
    d.finishedPriceFact,
    d.paymentSaleAmount,
    d.forPay,
    d.ComWBRub,
    d.ComWBPercent,
    d.Pur_price,
    d.Logistics_toclient_cost,
    d.Logistics_return_cost,
    d.Pur_date
FROM (
    SELECT
        seller_id,
        nmID,
        MIN(vendorCode)   AS vendorCode,
        MIN(subjectName)  AS subjectName,
        MIN(title)        AS title,
        MIN(photo)        AS photo
    FROM WB_products
    WHERE seller_id = %s
    GROUP BY seller_id, nmID
) AS p
LEFT JOIN WB_sellers AS s
       ON s.seller_id = p.seller_id
LEFT JOIN GS_RNP_ReportDetail AS d
       ON  d.seller_id = p.seller_id
       AND d.nmId      = p.nmID
       AND d.Date_order >= %s
       AND d.Date_order < %s
"""

TOTAL_NMID_STORAGE_FEE_BY_SELLER_SQL = """
SELECT
  seller_id,
  nmId,
  `date`,
  warehousePrice,
  barcodesCount,
  size,
  warehouse
FROM WB_storage_fee
WHERE seller_id = %s
  AND `date` >= %s
  AND `date` < %s
ORDER BY nmId, `date`
"""

TOTAL_NMID_AD_STATS_BY_SELLER_SQL = """
SELECT
  seller_id,
  advert_id,
  nmID,
  `date`,
  views,
  clicks,
  ctr,
  cpc,
  ad_expenses,
  atbs,
  orders,
  canceled,
  cr,
  shks,
  sum_price,
  app_type,
  avg_position
FROM WB_ad_stats
WHERE seller_id = %s
  AND `date` >= %s
  AND `date` < %s
ORDER BY advert_id, `date`
"""

TOTAL_NMID_STOCK_HISTORY_SQL = """
UPDATE GS_RNP_ReportDetail_daily_nmid AS r
JOIN (
  SELECT
    %s AS seller_id,
    %s AS nmid,
    m.year,
    m.month,
    COALESCE((
      SELECT JSON_ARRAYAGG(j)
      FROM (
        SELECT JSON_OBJECT(
                 'date',        DATE(sf.`date`),
                 'stocks_cnt',  SUM(sf.barcodesCount),
                 'storage_fee', SUM(sf.warehousePrice)
               ) AS j
        FROM WB_storage_fee sf
        WHERE sf.seller_id = %s
          AND sf.nmId      = %s
          AND YEAR(sf.`date`)  = m.year
          AND MONTH(sf.`date`) = m.month
        GROUP BY sf.`date`
        ORDER BY sf.`date`
      ) AS ordered_rows
    ), JSON_ARRAY()) AS stock_json
  FROM (
    SELECT r.year, r.month
    FROM GS_RNP_ReportDetail_daily_nmid r
    WHERE r.seller_id = %s
      AND r.nmid      = %s
    GROUP BY r.year, r.month
  ) AS m
  INNER JOIN (
    SELECT DISTINCT YEAR(sf.`date`) AS year, MONTH(sf.`date`) AS month
    FROM WB_storage_fee sf
    WHERE sf.seller_id = %s
      AND sf.nmId      = %s
  ) AS s
    ON s.year  = m.year
   AND s.month = m.month
) AS src
  ON  r.seller_id = src.seller_id
  AND r.nmid      = src.nmid
  AND r.year      = src.year
  AND r.month     = src.month
SET r.stock_history = src.stock_json
WHERE r.seller_id = %s
  AND r.nmid      = %s
"""

TOTAL_NMID_STOCK_TODAY_SQL = """
UPDATE GS_RNP_ReportDetail_daily_nmid AS r
JOIN (
  SELECT
    COALESCE(
      (
        SELECT JSON_ARRAYAGG(size_row)
        FROM (
          SELECT
            JSON_OBJECT(
              'techSize',       st.techSize,
              'quantity',       st.quantity,
              'inWayToClient',  st.inWayToClient,
              'inWayFromClient', st.inWayFromClient,
              'quantityFull',   st.quantityFull
            ) AS size_row
          FROM WB_stocks st
          WHERE st.seller_id = %s
            AND st.nmId      = %s
            AND COALESCE(
                  st.quantityFull,
                  st.quantity + st.inWayToClient + st.inWayFromClient,
                  0
                ) > 0
          ORDER BY st.techSize
        ) AS ordered_rows
      ),
      JSON_ARRAY()
    ) AS stock_json
) AS src
SET r.stock_today = src.stock_json
WHERE r.seller_id = %s
  AND r.nmid      = %s
"""

TOTAL_NMID_FUNNEL_METRICS_SQL = """
UPDATE GS_RNP_ReportDetail_daily_nmid AS r
JOIN (
  SELECT
      a.seller_id,
      a.nmID                         AS nmid,
      YEAR(a.dt)                     AS year,
      MONTH(a.dt)                    AS month,
      COALESCE((
        SELECT JSON_ARRAYAGG(day_row)
        FROM (
          SELECT JSON_OBJECT(
                   'date',                 DATE(a2.dt),
                   'openCardCount',        a2.openCardCount,
                   'addToCartCount',       a2.addToCartCount,
                   'addToCartConversion',  a2.addToCartConversion,
                   'ordersCount',          a2.ordersCount,
                   'ordersSumRub',         a2.ordersSumRub,
                   'cartToOrderConversion', a2.cartToOrderConversion,
                   'buyoutsCount',         a2.buyoutsCount,
                   'buyoutsSumRub',        a2.buyoutsSumRub,
                   'buyoutPercent',        a2.buyoutPercent,
                   'addToWishlistCount',   a2.addToWishlistCount  
                 ) AS day_row
          FROM WB_analytics a2
          WHERE a2.seller_id = %s
            AND a2.nmID      = %s
            AND YEAR(a2.dt)  = YEAR(a.dt)
            AND MONTH(a2.dt) = MONTH(a.dt)
          ORDER BY a2.dt
        ) AS ordered_rows
      ), JSON_ARRAY()) AS funnel_json
  FROM WB_analytics a
  WHERE a.seller_id = %s
    AND a.nmID      = %s
  GROUP BY a.seller_id, a.nmID, YEAR(a.dt), MONTH(a.dt)
) AS src
  ON  r.seller_id = src.seller_id
  AND r.nmid      = src.nmid
  AND r.year      = src.year
  AND r.month     = src.month
SET r.funnel_metrics = src.funnel_json
WHERE r.seller_id = %s
  AND r.nmid      = %s
"""


def fetch_total_nmid_cards(
    cursor: mysql.connector.cursor.MySQLCursorDict,
    seller_id: str,
) -> List[Dict]:
    cursor.execute(TOTAL_NMID_PRODUCTS_SQL, (seller_id,))
    rows = cursor.fetchall()
    result: List[Dict] = []
    for row in rows:
        card = dict(row)
        card["seller_id"] = seller_id
        card["orders_status"] = parse_json_field(card.get("orders_status"))
        card["sales_status"] = parse_json_field(card.get("sales_status"))
        card["total_nmid_status"] = parse_json_field(card.get("total_nmid_status"))
        card["paid_storage_status"] = parse_json_field(card.get("paid_storage_status"))
        card["ad_stats_status"] = parse_json_field(card.get("ad_stats_status"))
        result.append(card)
    return result


def fetch_total_nmid_rows(
    cursor: mysql.connector.cursor.MySQLCursorDict,
    seller_id: str,
    nmid: Any,
    date_from: str,
    date_to: str,
) -> List[Dict]:
    cursor.execute(
        TOTAL_NMID_FETCH_SQL,
        (
            seller_id,
            nmid,
            date_from,
            date_to,
        ),
    )
    return cursor.fetchall()


def fetch_total_nmid_rows_by_seller(
    cursor: mysql.connector.cursor.MySQLCursorDict,
    seller_id: str,
    date_from: str,
    date_to: str,
) -> List[Dict]:
    """Выгружает все данные по селлеру за период (все артикулы одним запросом)"""
    cursor.execute(
        TOTAL_NMID_FETCH_BY_SELLER_SQL,
        (
            seller_id,
            date_from,
            date_to,
        ),
    )
    return cursor.fetchall()


def get_stock_history_date_range(
    paid_storage_status: Optional[Dict],
    today: date,
) -> tuple[str, str]:
    """
    Определяет период для выгрузки WB_storage_fee на основе paid_storage_status.
    Возвращает (date_from, date_to).
    """
    tomorrow = today + timedelta(days=1)
    
    if isinstance(paid_storage_status, dict):
        max_begin_date = paid_storage_status.get("maxBeginDate")
        if max_begin_date:
            max_date = parse_ymd_date(max_begin_date)
            if max_date:
                # Берём от maxBeginDate до сегодня
                date_from = first_day_of_month(max_date)
                date_to = tomorrow.isoformat()
                return (date_from.isoformat(), date_to)
    
    # Если нет maxBeginDate - берём последние 15 дней (с захватом полного месяца)
    start_candidate = today - timedelta(days=15)
    date_from = first_day_of_month(start_candidate)
    date_to = tomorrow.isoformat()
    return (date_from.isoformat(), date_to)


def fetch_storage_fee_by_seller(
    cursor: mysql.connector.cursor.MySQLCursorDict,
    seller_id: str,
    date_from: str,
    date_to: str,
) -> List[Dict]:
    """Выгружает все строки WB_storage_fee по селлеру за период"""
    cursor.execute(
        TOTAL_NMID_STORAGE_FEE_BY_SELLER_SQL,
        (
            seller_id,
            date_from,
            date_to,
        ),
    )
    return cursor.fetchall()


def get_ad_stats_date_range(
    ad_stats_status: Optional[Dict],
    today: date,
) -> tuple[str, str]:
    """
    Определяет период для выгрузки WB_ad_stats на основе ad_stats_status.
    Возвращает (date_from, date_to).
    """
    tomorrow = today + timedelta(days=1)
    
    if isinstance(ad_stats_status, dict):
        max_begin_date = ad_stats_status.get("maxBeginDate")
        if max_begin_date:
            max_date = parse_ymd_date(max_begin_date)
            if max_date:
                # Берём от maxBeginDate до сегодня
                date_from = first_day_of_month(max_date)
                date_to = tomorrow.isoformat()
                return (date_from.isoformat(), date_to)
    
    # Если нет maxBeginDate - берём последние 15 дней (с захватом полного месяца)
    start_candidate = today - timedelta(days=15)
    date_from = first_day_of_month(start_candidate)
    date_to = tomorrow.isoformat()
    return (date_from.isoformat(), date_to)


def fetch_ad_stats_by_seller(
    cursor: mysql.connector.cursor.MySQLCursorDict,
    seller_id: str,
    date_from: str,
    date_to: str,
) -> List[Dict]:
    """Выгружает все строки WB_ad_stats по селлеру за период"""
    cursor.execute(
        TOTAL_NMID_AD_STATS_BY_SELLER_SQL,
        (
            seller_id,
            date_from,
            date_to,
        ),
    )
    return cursor.fetchall()


def build_total_nmid_chunks_for_card(card: Dict, today: date) -> List[Dict]:
    tomorrow = today + timedelta(days=1)
    total_status = card.get("total_nmid_status")
    orders_status = card.get("orders_status")
    sales_status = card.get("sales_status")

    def build_chunks_from_start(start_date: date, months_per_chunk: int) -> List[Dict]:
        chunks: List[Dict] = []
        current_start = first_day_of_month(start_date)
        if current_start > today:
            current_start = first_day_of_month(today)
        while current_start <= today:
            next_start = add_months_first_day(current_start, months_per_chunk)
            date_to = next_start if next_start <= today else tomorrow
            chunks.append(
                {
                    "date_from": current_start.isoformat(),
                    "date_to": date_to.isoformat(),
                }
            )
            if next_start > today:
                break
            current_start = next_start
        if not chunks:
            chunks.append(
                {
                    "date_from": first_day_of_month(today).isoformat(),
                    "date_to": tomorrow.isoformat(),
                }
            )
        return chunks

    def build_recent_chunks(window_days: int) -> List[Dict]:
        start_candidate = today - timedelta(days=window_days)
        start_date = first_day_of_month(start_candidate)
        return build_chunks_from_start(start_date, 1)

    if isinstance(total_status, dict) and (total_status.get("status") or "").lower() == "right":
        return build_recent_chunks(15)

    earliest_dates: List[date] = []
    for status_obj in (orders_status, sales_status):
        if isinstance(status_obj, dict):
            dt = parse_ymd_date(status_obj.get("maxDateFrom"))
            if dt:
                earliest_dates.append(dt)

    if earliest_dates:
        earliest = max(min(earliest_dates), date(2025, 1, 1))
        return build_chunks_from_start(earliest, 3)

    return build_recent_chunks(15)


def _float_or_zero(value: Any) -> float:
    try:
        return float(value) if value is not None else 0.0
    except (TypeError, ValueError):
        return 0.0


def _float_or_none(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def normalize_date_value(value: Any) -> str:
    """Нормализует значение даты в строку формата YYYY-MM-DD"""
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            return str(value)
    return str(value)


def aggregate_total_nmid_rows(
    card: Dict,
    rows: List[Dict],
    date_from: str,
    date_to: str,
) -> List[Dict]:
    seller_id = str(card.get("seller_id") or "")
    nmid = card.get("nmid") or card.get("nmID")
    card_brand = card.get("wb_api_brand") or ""
    card_subject = card.get("subjectName") or card.get("Subject") or ""
    card_title = card.get("title") or ""
    card_photo = card.get("photo") or ""
    card_supplier = card.get("supplierArticle") or card.get("vendorCode") or ""

    range_start = parse_ymd_date(date_from) or date.today()
    range_end_exclusive = parse_ymd_date(date_to) or (range_start + timedelta(days=1))
    range_end = range_end_exclusive - timedelta(days=1)
    if range_end < range_start:
        range_end = range_start

    groups: Dict[Tuple[str, Any, int, int], Dict[str, Any]] = {}

    for row in rows:
        row_seller = row.get("seller_id") or seller_id
        row_nmid = row.get("nmID") or row.get("nmid") or nmid
        row_brand = row.get("wb_api_brand") or card_brand
        row_subject = row.get("Subject") or card_subject
        row_title = row.get("title") or card_title
        row_photo = row.get("photo") or card_photo
        row_supplier = row.get("supplierArticle") or row.get("vendorCode") or card_supplier

        date_fields = ["Date_order", "date_sales", "Date_cancel", "date_return"]
        months_set: Set[Tuple[int, int]] = set()
        for field in date_fields:
            dt = parse_ymd_date(row.get(field))
            if dt:
                months_set.add((dt.year, dt.month))

        if not months_set:
            for month_start in month_span(range_start, range_end):
                months_set.add((month_start.year, month_start.month))

        for year, month in months_set:
            key = (row_seller, row_nmid, year, month)
            entry = groups.setdefault(
                key,
                {
                    "meta": {
                        "seller_nmid_month_year_key": f"{row_seller}_{row_nmid}_{str(month).zfill(2)}_{year}",
                        "month": month,
                        "year": year,
                        "seller_id": row_seller,
                        "wb_api_brand": row_brand or card_brand,
                        "Subject": row_subject or card_subject,
                        "nmid": row_nmid,
                        "title": row_title or card_title,
                        "photo": row_photo or card_photo,
                        "supplierArticle": str(row_supplier or card_supplier or ""),
                    },
                    "orders": [],
                },
            )
            entry["orders"].append(row)

    # гарантируем, что каждый месяц в диапазоне добавлен (даже без данных)
    for month_start in month_span(range_start, range_end):
        key = (seller_id, nmid, month_start.year, month_start.month)
        if key not in groups:
            groups[key] = {
                "meta": {
                    "seller_nmid_month_year_key": f"{seller_id}_{nmid}_{str(month_start.month).zfill(2)}_{month_start.year}",
                    "month": month_start.month,
                    "year": month_start.year,
                    "seller_id": seller_id,
                    "wb_api_brand": card_brand,
                    "Subject": card_subject,
                    "nmid": nmid,
                    "title": card_title,
                    "photo": card_photo,
                    "supplierArticle": str(card_supplier or ""),
                },
                "orders": [],
            }

    outputs: List[Dict] = []

    for entry in groups.values():
        meta = entry["meta"]
        orders_list = entry["orders"]
        month = meta["month"]
        year = meta["year"]
        first = date(year, month, 1)
        last = last_day_of_month(first)

        def days_in_month() -> List[date]:
            current = first
            result: List[date] = []
            while current <= last:
                result.append(current)
                current += timedelta(days=1)
            return result

        def filter_by(field: str, day_str: str) -> List[Dict]:
            return [
                o
                for o in orders_list
                if normalize_date_value(o.get(field)).startswith(day_str)
            ]

        def sum_field(iterable: Iterable[Dict], field: str) -> float:
            return sum(_float_or_zero(item.get(field)) for item in iterable)

        def avg_field(iterable: Iterable[Dict], field: str) -> Optional[float]:
            values = [_float_or_none(item.get(field)) for item in iterable]
            values = [v for v in values if v is not None]
            if not values:
                return None
            return sum(values) / len(values)

        def calc_spp(iterable: Iterable[Dict]) -> Optional[float]:
            values = list(iterable)
            if not values:
                return None
            acc = 0.0
            count = 0
            for item in values:
                pre = _float_or_zero(item.get("priceWithDisc"))
                post = _float_or_zero(item.get("finishedPrice"))
                if pre:
                    acc += (pre - post) / pre
                    count += 1
            if not count:
                return None
            return round((acc / count) * 100, 2)

        day_metrics: List[Dict] = []
        for day in days_in_month():
            day_str = day.isoformat()
            ord_rows = filter_by("Date_order", day_str)
            ord_buy = [o for o in ord_rows if o.get("status") == "Выкуп"]
            ord_canc = [o for o in ord_rows if o.get("status") == "Отмена"]
            ord_ret = [o for o in ord_rows if o.get("status") == "Возврат"]
            in_way = len([o for o in ord_rows if o.get("status") == "Доставка"])

            fact_sales = [
                o
                for o in orders_list
                if o.get("status") == "Выкуп"
                and normalize_date_value(o.get("date_sales")).startswith(day_str)
            ]
            fact_canc = [
                o
                for o in orders_list
                if o.get("status") == "Отмена"
                and normalize_date_value(o.get("Date_cancel")).startswith(day_str)
            ]
            fact_ret = [
                o
                for o in orders_list
                if o.get("status") == "Возврат"
                and normalize_date_value(o.get("date_return")).startswith(day_str)
            ]

            buy_cnt = len(ord_buy)
            canc_cnt = len(ord_canc)
            ret_cnt = len(ord_ret)
            denom = buy_cnt + canc_cnt + ret_cnt

            def pct(num: int) -> float:
                return round((num / denom) * 100, 2) if denom else 0.0

            day_metrics.append(
                {
                    "date": day_str,
                    "orders_sum_pre": sum_field(ord_rows, "priceWithDisc"),
                    "orders_sum_post": sum_field(ord_rows, "finishedPrice"),
                    "orders_cnt": len(ord_rows),
                    "log_to": sum_field(ord_rows, "Logistics_toclient_cost"),
                    "cancel_perc": pct(canc_cnt),
                    "sales_buyout": pct(buy_cnt),
                    "returns_perc": pct(ret_cnt),
                    "fact_sales_cnt": len(fact_sales),
                    "fact_sales_prep_rev": sum_field(fact_sales, "priceWithDisc"),
                    "fact_sales_post_rev": sum_field(fact_sales, "finishedPrice"),
                    "fact_commission_sum": sum_field(fact_sales, "ComWBRub"),
                    "fact_commission_perc": avg_field(fact_sales, "ComWBPercent"),
                    "fact_cancels_cnt": len(fact_canc),
                    "fact_cancels_sum": sum_field(fact_canc, "finishedPrice"),
                    "fact_returns_cnt": len(fact_ret),
                    "fact_returns_sum": sum_field(fact_ret, "finishedPrice"),
                    "fact_log_from": sum_field(fact_canc, "Logistics_return_cost") + sum_field(fact_ret, "Logistics_return_cost"),
                    "fact_cogs": sum_field(fact_sales, "Pur_price"),
                    "fact_forpay": sum_field(fact_sales, "forPay"),
                    "fact_spp": calc_spp(fact_sales),
                    "ord_sales_cnt": len(ord_buy),
                    "ord_sales_prep_rev": sum_field(ord_buy, "priceWithDisc"),
                    "ord_sales_post_rev": sum_field(ord_buy, "finishedPrice"),
                    "ord_commission_sum": sum_field(ord_buy, "ComWBRub"),
                    "ord_commission_perc": avg_field(ord_buy, "ComWBPercent"),
                    "ord_cancels_cnt": len(ord_canc),
                    "ord_cancels_sum": sum_field(ord_canc, "finishedPrice"),
                    "ord_returns_cnt": len(ord_ret),
                    "ord_returns_sum": sum_field(ord_ret, "finishedPrice"),
                    "ord_log_from": sum_field(ord_canc, "Logistics_return_cost") + sum_field(ord_ret, "Logistics_return_cost"),
                    "ord_cogs": sum_field(ord_buy, "Pur_price"),
                    "ord_forpay": sum_field(ord_buy, "forPay"),
                    "ord_spp": calc_spp(ord_rows),
                    "in_way": in_way,
                }
            )

        outputs.append({**meta, "day_metrics": day_metrics})

    return outputs

def prepare_stocks_requests(sellers: Iterable[Dict]) -> Tuple[List[Dict], List[Dict], str]:
    processed: List[Dict] = []
    
    for idx, raw in enumerate(sellers):
        seller_id = str(raw.get("seller_id") or "").strip()
        if not seller_id:
            continue
        brand = (raw.get("wb_api_brand") or seller_id).strip()
        token = (raw.get("wb_api_key") or "").strip()
        
        # Читаем статус (парсим JSON если нужно)
        raw_status = parse_json_field(raw.get("stock_status"))
        status_obj = raw_status if isinstance(raw_status, dict) else None
        now_time = status_obj.get("nowTime") if isinstance(status_obj, dict) else None
        
        # Определяем статус по наличию nowTime: если есть - был прогнан (right), если нет - новичок (new)
        if status_obj is None or not now_time:
            status_cat = "new"
        else:
            # Если есть nowTime - значит был прогнан, используем right для кулдауна
            status_cat = "right"
        
        processed.append(
            {
                "seller_id": seller_id,
                "brand": brand,
                "token": token,
                "idx": idx,
                "status_cat": status_cat,
                "now_time": now_time,
                "stock_status": status_obj,
                "stock_status_raw": raw.get("stock_status"),
            }
        )
    
    def parse_now_time(value: Optional[str]) -> float:
        dt = parse_msk_datetime(value)
        if dt is None:
            return float("inf")
        return dt.timestamp()
    
    def stocks_right_cooldown_minutes(status_obj: Optional[Dict]) -> float:
        if not status_obj:
            return 0.0
        now_time_str = status_obj.get("nowTime")
        dt = parse_msk_datetime(now_time_str)
        if dt is None:
            return 0.0
        age_min = minutes_since_msk(dt)
        remaining = STOCKS_RIGHT_COOLDOWN_MINUTES - age_min
        return max(0.0, remaining)
    
    new_group = [s for s in processed if s["status_cat"] == "new"]
    right_group = [s for s in processed if s["status_cat"] == "right"]
    
    allow_set: Set[str] = set()
    if new_group:
        # Если есть новички - обрабатываем только их
        allow_set.update(s["seller_id"] for s in new_group)
    else:
        # Если новичков нет - обрабатываем right с истекшим кулдауном
        for s in right_group:
            cooldown_remaining = stocks_right_cooldown_minutes(s.get("stock_status"))
            if cooldown_remaining <= 0:
                allow_set.add(s["seller_id"])
    
    summary_lines = []
    for s in sorted(processed, key=lambda item: item["brand"].lower()):
        allowed = s["seller_id"] in allow_set
        mark = "✅" if allowed else "✖️"
        status_dict = s.get("stock_status") or {}
        now_dt = parse_msk_datetime(status_dict.get("nowTime") or s.get("now_time"))
        if s["status_cat"] == "new":
            minutes_text = "new"
        elif now_dt is not None:
            age_min = max(0, int(minutes_since_msk(now_dt)))
            minutes_text = f"{age_min}min"
        else:
            minutes_text = "--"
        summary_lines.append(f"{minutes_text} | {s['brand']} {mark}")
    summary_text = "\n".join(summary_lines)
    
    allowed_effective = [
        s for s in processed if s["seller_id"] in allow_set
    ]
    
    requests: List[Dict] = []
    total_sellers = len(allowed_effective)
    
    for seller_index, s in enumerate(sorted(allowed_effective, key=lambda item: item["brand"].lower()), start=1):
        requests.append(
            {
                "seller_id": s["seller_id"],
                "brand": s["brand"],
                "token": s["token"],
                "seller_index": seller_index,
                "sellers_total": total_sellers,
                "stock_status": s.get("stock_status"),
            }
        )
    
    return requests, processed, summary_text


def fetch_ad_stats_sellers(cursor: mysql.connector.cursor.MySQLCursorDict) -> List[Dict]:
    cursor.execute(
        """
        SELECT
            s.seller_id,
            s.wb_api_key,
            s.wb_api_brand,
            su.ad_stats_status,
            COALESCE((
                SELECT JSON_ARRAYAGG(j.campaign_json)
                FROM (
                    SELECT JSON_OBJECT(
                        'advertId', c.advertId,
                        'type', c.`type`,
                        'status', c.`status`
                    ) AS campaign_json
                    FROM WB_ad_campaigns AS c
                    WHERE c.seller_id = s.seller_id
                    ORDER BY c.changeTime DESC
                ) AS j
            ), JSON_ARRAY()) AS campaigns
        FROM WB_sellers AS s
        JOIN WB_sellers_updates AS su
            ON su.seller_id = s.seller_id
        WHERE su.in_workrnp = 1
        """
    )
    rows = cursor.fetchall()
    result: List[Dict] = []
    for row in rows:
        raw_status = parse_json_field(row.get("ad_stats_status"))
        normalized_status = None
        if isinstance(raw_status, list):
            for item in reversed(raw_status):
                if isinstance(item, dict):
                    normalized_status = item
                    break
        elif isinstance(raw_status, dict):
            normalized_status = raw_status
        row["ad_stats_status_raw"] = raw_status
        row["ad_stats_status"] = normalized_status

        campaigns_raw = parse_json_field(row.get("campaigns"))
        row["campaigns"] = campaigns_raw if isinstance(campaigns_raw, list) else []
        result.append(row)
    return result


def fetch_ad_expenses_sellers(cursor: mysql.connector.cursor.MySQLCursorDict) -> List[Dict]:
    cursor.execute(
        """
        SELECT
          s.seller_id,
          s.wb_api_key,
          s.wb_api_brand,
          su.ad_expenses_status,
          COALESCE(c.campaigns_cnt, 0) AS campaigns_cnt
        FROM WB_sellers AS s
        JOIN WB_sellers_updates AS su
          ON su.seller_id = s.seller_id
        LEFT JOIN (
          SELECT seller_id, COUNT(*) AS campaigns_cnt
          FROM WB_ad_campaigns
          GROUP BY seller_id
        ) AS c
          ON c.seller_id = s.seller_id
        WHERE su.in_workrnp = 1
        """
    )
    rows = cursor.fetchall()
    result: List[Dict] = []
    for row in rows:
        raw_status = parse_json_field(row.get("ad_expenses_status"))
        normalized_status = None
        if isinstance(raw_status, list):
            for item in reversed(raw_status):
                if isinstance(item, dict):
                    normalized_status = item
                    break
        elif isinstance(raw_status, dict):
            normalized_status = raw_status
        row["ad_expenses_status_raw"] = raw_status
        row["ad_expenses_status"] = normalized_status
        # Если campaigns_cnt = 0, сразу ставим статус right
        campaigns_cnt = int(row.get("campaigns_cnt", 0) or 0)
        if campaigns_cnt == 0 and normalized_status is None:
            row["ad_expenses_status"] = {
                "status": "right",
                "nowTime": msk_now().strftime("%Y-%m-%d %H:%M:%S"),
                "lastTotalRow": 0,
                "leftBoundary": "",
                "maxBeginDate": "",
                "lastBeginDate": "",
                "rightBoundary": "",
            }
        result.append(row)
    return result


def build_ad_stats_selection_message(summary_text: str) -> str:
    escaped = html.escape(summary_text)
    return (
        "<b>05 WB API</b> | Ad Stats\n"
        "<blockquote>Подготовка выгрузки статистики по рекламным кампаниям.\n"
        f"<code>{escaped}</code></blockquote>"
    )


def parse_ad_stats_payload(payload: Any) -> List[Dict]:
    if isinstance(payload, list):
        payload_items = [item for item in payload if isinstance(item, dict)]
    elif isinstance(payload, dict):
        payload_items = [payload]
    else:
        return []

    parsed_rows: List[Dict] = []
    for item in payload_items:
        advert_id = to_int_or_none(item.get("advertId") or item.get("advert_id"))
        if advert_id is None:
            continue

        booster_index = build_booster_index(item.get("boosterStats"))
        days = item.get("days")
        if not isinstance(days, list) or not days:
            continue

        for day in days:
            if not isinstance(day, dict):
                continue
            date_str = to_ymd_from_iso(day.get("date"))
            if not date_str:
                continue

            apps = day.get("apps")
            if not isinstance(apps, list):
                continue

            for app in apps:
                if not isinstance(app, dict):
                    continue
                app_type = to_int_or_none(app.get("appType") or app.get("app_type"))
                # Если appType отсутствует, используем значение по умолчанию 99 (не 0, т.к. AD_STATS_SKIP_APP_TYPE_ZERO пропускает 0)
                if app_type is None:
                    app_type = 99
                if AD_STATS_SKIP_APP_TYPE_ZERO and app_type == 0:
                    continue

                nms = app.get("nms")
                if not isinstance(nms, list):
                    continue

                for nm in nms:
                    if not isinstance(nm, dict):
                        continue
                    nm_id = extract_nm_id(nm)
                    if nm_id is None:
                        continue

                    key = f"{date_str}|{nm_id}"
                    parsed_rows.append(
                        {
                            "advertId": advert_id,
                            "date": date_str,
                            "nmId": nm_id,
                            "appType": app_type,
                            "views": nm.get("views"),
                            "clicks": nm.get("clicks"),
                            "ctr": nm.get("ctr"),
                            "cpc": nm.get("cpc"),
                            "atbs": nm.get("atbs"),
                            "orders": nm.get("orders"),
                            "shks": nm.get("shks"),
                            "cr": nm.get("cr"),
                            "canceled": nm.get("canceled"),
                            "sum": nm.get("sum"),
                            "sum_price": nm.get("sum_price"),
                            "ad_expenses": nm.get("sum"),  # расходы на рекламу = sum
                            "avg_position": booster_index.get(key),
                        }
                    )

    return parsed_rows


def build_ad_stats_plan_message(requests: List[Dict], sellers_count: int) -> str:
    lines = [f"Запросов: {len(requests)} | селлеров: {sellers_count}"]
    sorted_requests = sorted(requests, key=lambda r: r.get("ready_at_ts", 0.0))
    preview = sorted_requests[:50]
    for idx, entry in enumerate(preview, start=1):
        begin_short = format_date_short(entry["interval"]["beginDate"])
        end_short = format_date_short(entry["interval"]["endDate"])
        lines.append(
            f"{idx}) chunk {entry['chunk_index']}/{entry['chunk_total']} | "
            f"{begin_short}→{end_short} | {entry['brand']}"
        )
    if len(requests) > len(preview):
        lines.append("...")

    body = "\n".join(lines)
    return (
        "<b>05 WB API</b> | Ad Stats\n"
        "<blockquote><code>"
        f"{html.escape(body)}"
        "</code></blockquote>"
    )


def write_ad_list_csv(rows: List[Dict], path: str) -> None:
    import csv
    from pathlib import Path

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)

    with target.open("w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=AD_LIST_CSV_COLUMNS)
        writer.writeheader()
        for chunk_start in range(0, len(rows), 40000):
            chunk = rows[chunk_start : chunk_start + 40000]
            writer.writerows(chunk)


def write_ad_stats_csv(rows: List[Dict], path: str) -> None:
    import csv
    from pathlib import Path

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)

    with target.open("w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=AD_STATS_CSV_COLUMNS)
        writer.writeheader()
        for chunk_start in range(0, len(rows), 40000):
            chunk = rows[chunk_start : chunk_start + 40000]
            writer.writerows(chunk)


def process_ad_expenses_response(response_data: List[Dict], seller_id: str) -> List[Dict]:
    """Обрабатывает ответ от WB API /adv/v1/upd и группирует по seller_id + advertId + updDate + paymentType"""
    grouped: Dict[str, Dict] = {}
    
    for item in response_data:
        advert_id = item.get("advertId")
        if advert_id is None:
            continue
        
        advert_id_str = str(advert_id).strip()
        payment_type = str(item.get("paymentType", "")).strip()
        
        # Извлекаем дату из updTime (может быть null)
        upd_time_raw = item.get("updTime")
        if upd_time_raw:
            upd_time_str = str(upd_time_raw)
            # Извлекаем дату (первые 10 символов ISO формата)
            upd_date = upd_time_str[:10] if len(upd_time_str) >= 10 else ""
        else:
            # Если updTime null, используем текущую дату
            upd_date = ymd(msk_now())
            upd_time_str = ""
        
        if not upd_date:
            continue
        
        # Уникальный ключ: seller_id + advertId + date + paymentType
        key = f"{seller_id}_{advert_id_str}_{upd_date}_{payment_type}"
        
        if key not in grouped:
            grouped[key] = {
                "seller_id": seller_id,
                "advertId": advert_id_str,
                "campName": str(item.get("campName", "")).strip(),
                "advertType": int(item.get("advertType", 0) or 0),
                "paymentType": payment_type,
                "advertStatus": int(item.get("advertStatus", 0) or 0),
                "updNum": int(item.get("updNum", 0) or 0),
                "updTime": upd_date if upd_time_str else "",  # Сохраняем только дату или пустую строку
                "updSum": float(item.get("updSum", 0) or 0),
            }
        else:
            # Суммируем updSum при группировке
            grouped[key]["updSum"] += float(item.get("updSum", 0) or 0)
    
    result = []
    for key, row in grouped.items():
        row["seller_advert_date_key"] = key
        result.append(row)
    
    return result


def write_ad_expenses_csv(rows: List[Dict], path: str) -> None:
    import csv
    from pathlib import Path

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)

    with target.open("w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=AD_EXPENSES_CSV_COLUMNS)
        writer.writeheader()
        for chunk_start in range(0, len(rows), 40000):
            chunk = rows[chunk_start : chunk_start + 40000]
            writer.writerows(chunk)


def create_stocks_report(token: str) -> Optional[str]:
    """Создает отчет по остаткам и возвращает task_id"""
    params = {
        "groupByBrand": "true",
        "groupBySubject": "true",
        "groupBySa": "true",
        "groupByNm": "true",
        "groupByBarcode": "true",
        "groupBySize": "true",
    }
    headers = {
        "user-agent": WB_USER_AGENT,
        "Authorization": f"Bearer {token}",
    }
    
    try:
        response = requests.get(WB_STOCKS_CREATE_URL, params=params, headers=headers, timeout=40)
        if response.status_code == 200:
            data = response.json()
            if isinstance(data, dict) and "data" in data:
                task_id = data["data"].get("taskId")
                if task_id:
                    return str(task_id).strip()
        return None
    except Exception:
        return None


def aggregate_stock_history_rows(
    storage_fee_rows: List[Dict],
    existing_records: Dict[str, Dict],
) -> List[Dict]:
    """
    Агрегирует строки WB_storage_fee по (seller_id, nmid, year, month)
    и формирует stock_history JSON для каждого месяца.
    
    Args:
        storage_fee_rows: список строк из WB_storage_fee
        existing_records: словарь существующих записей из GS_RNP_ReportDetail_daily_nmid
                          ключ: seller_nmid_month_year_key, значение: запись с year, month
    
    Returns:
        список записей для обновления с полями:
        - seller_nmid_month_year_key
        - seller_id, nmid, year, month
        - stock_history (JSON массив)
    """
    # Группировка: key = "seller_id|nmid|year|month"
    groups: Dict[str, Dict] = {}
    
    for row in storage_fee_rows:
        seller_id = str(row.get("seller_id") or "")
        nmid = row.get("nmId")
        if not seller_id or nmid is None:
            continue
        
        date_str_raw = str(row.get("date") or "")[:10]
        if not date_str_raw or len(date_str_raw) < 10:
            continue
        
        try:
            parts = date_str_raw.split("-")
            if len(parts) != 3:
                continue
            year = int(parts[0])
            month = int(parts[1])  # 1..12
        except (ValueError, IndexError):
            continue
        
        barcodes_count = _float_or_zero(row.get("barcodesCount"))
        warehouse_price = _float_or_zero(row.get("warehousePrice"))
        size = str(row.get("size") or "").strip()
        warehouse_name = str(row.get("warehouse") or "").strip()
        
        group_key = f"{seller_id}|{nmid}|{year}|{month}"
        
        if group_key not in groups:
            groups[group_key] = {
                "seller_id": seller_id,
                "nmid": nmid,
                "year": year,
                "month": month,
                "_days": {},  # dateStr -> агрегат по дню
            }
        
        group = groups[group_key]
        
        if date_str_raw not in group["_days"]:
            group["_days"][date_str_raw] = {
                "date": date_str_raw,
                "stocks_cnt": 0.0,
                "storage_fee": 0.0,
                "_sizes": {},  # size -> stocks_cnt
                "_warehouses": {},  # warehouse -> { warehouse, storage_fee, stocks_cnt }
            }
        
        day = group["_days"][date_str_raw]
        
        # Общие по дню
        day["stocks_cnt"] += barcodes_count
        day["storage_fee"] += warehouse_price
        
        # Детализация по размерам
        if size:
            if size not in day["_sizes"]:
                day["_sizes"][size] = 0.0
            day["_sizes"][size] += barcodes_count
        
        # Детализация по складам
        if warehouse_name:
            if warehouse_name not in day["_warehouses"]:
                day["_warehouses"][warehouse_name] = {
                    "warehouse": warehouse_name,
                    "storage_fee": 0.0,
                    "stocks_cnt": 0.0,
                }
            wh = day["_warehouses"][warehouse_name]
            wh["storage_fee"] += warehouse_price
            wh["stocks_cnt"] += barcodes_count
    
    # Формируем финальный результат
    aggregated = []
    
    for group_key, group in groups.items():
        seller_id = group["seller_id"]
        nmid = group["nmid"]
        year = group["year"]
        month = group["month"]
        
        seller_nmid_month_year_key = f"{seller_id}_{nmid}_{str(month).zfill(2)}_{year}"
        
        # Проверяем, есть ли такая запись в existing_records (только те месяцы, где есть продажи)
        if seller_nmid_month_year_key not in existing_records:
            continue
        
        day_keys = sorted(group["_days"].keys())  # сортировка по дате
        stock_history = []
        
        for date_str in day_keys:
            day = group["_days"][date_str]
            
            # sizes
            sizes_arr = []
            if day["_sizes"]:
                for size_key in sorted(day["_sizes"].keys(), key=lambda x: str(x)):
                    sizes_arr.append({
                        "size": size_key,
                        "stocks_cnt": int(day["_sizes"][size_key]),
                    })
            
            # warehouses
            warehouses_arr = []
            if day["_warehouses"]:
                for wh in sorted(day["_warehouses"].values(), key=lambda x: str(x.get("warehouse", ""))):
                    warehouses_arr.append({
                        "warehouse": wh["warehouse"],
                        "storage_fee": float(wh["storage_fee"]),
                        "stocks_cnt": int(wh["stocks_cnt"]),
                    })
            
            day_obj: Dict[str, Any] = {
                "date": day["date"],
                "stocks_cnt": int(day["stocks_cnt"]),
                "storage_fee": float(day["storage_fee"]),
            }
            
            if sizes_arr:
                day_obj["sizes"] = sizes_arr
            
            if warehouses_arr:
                day_obj["warehouses"] = warehouses_arr
            
            stock_history.append(day_obj)
        
        aggregated.append({
            "seller_nmid_month_year_key": seller_nmid_month_year_key,
            "seller_id": seller_id,
            "nmid": nmid,
            "year": year,
            "month": month,
            "stock_history": stock_history,
        })
    
    return aggregated


def aggregate_ad_stats_rows(ad_stats_rows: List[Dict]) -> List[Dict]:
    """
    Агрегирует строки WB_ad_stats по (seller_id, advert_id, year, month)
    и формирует day_adstats JSON для каждого месяца.
    
    Args:
        ad_stats_rows: список строк из WB_ad_stats
    
    Returns:
        список записей для обновления с полями:
        - seller_adid_month_year_key
        - seller_id, advert_id, nmid, year, month
        - day_adstats (JSON массив)
    """
    def to_number(v: Any, default: float = 0.0) -> float:
        if v is None or v == "":
            return default
        try:
            n = float(v)
            # Проверка на NaN и Infinity (как Number.isFinite в JS)
            if n != n or not (-float('inf') < n < float('inf')):
                return default
            return n
        except (TypeError, ValueError):
            return default
    
    def round_value(v: float, decimals: int = 3) -> float:
        factor = 10 ** decimals
        return round(v * factor) / factor
    
    # Группировка: key = "seller_id|advert_id|year|month"
    groups: Dict[str, Dict] = {}
    
    skipped_count = 0
    for row in ad_stats_rows:
        seller_id = str(row.get("seller_id") or "")
        advert_id = row.get("advert_id")
        nmid = row.get("nmID")
        
        if not seller_id or advert_id is None or nmid is None:
            skipped_count += 1
            continue
        
        date_raw = str(row.get("date") or "")[:10]
        if not date_raw or len(date_raw) < 10:
            continue
        
        try:
            parts = date_raw.split("-")
            if len(parts) != 3:
                continue
            year = int(parts[0])
            month = int(parts[1])  # 1..12
            if not year or not month:
                continue
        except (ValueError, IndexError):
            continue
        
        group_key = f"{seller_id}|{advert_id}|{year}|{month}"
        
        if group_key not in groups:
            groups[group_key] = {
                "seller_id": seller_id,
                "advert_id": advert_id,
                "year": year,
                "month": month,
                "nmid": nmid,
                "days": {},  # dateStr -> агрегатор по дню
            }
        
        group = groups[group_key]
        
        if date_raw not in group["days"]:
            group["days"][date_raw] = {
                "date": date_raw,
                # суммы
                "views_sum": 0.0,
                "clicks_sum": 0.0,
                "adexp_sum": 0.0,
                "atbs_sum": 0.0,
                "orders_sum_cnt": 0.0,
                "shks_sum": 0.0,
                "orders_money_sum": 0.0,
                # для средних
                "ctr_sum": 0.0,
                "cpc_sum": 0.0,
                "cr_sum": 0.0,
                "avg_pos_sum": 0.0,
                "rows_cnt": 0,
            }
        
        day = group["days"][date_raw]
        
        views = to_number(row.get("views"))
        clicks = to_number(row.get("clicks"))
        adexp = to_number(row.get("ad_expenses"))
        atbs = to_number(row.get("atbs"))
        orders_cnt = to_number(row.get("orders"))
        shks = to_number(row.get("shks"))
        orders_money = to_number(row.get("sum_price"))
        ctr = to_number(row.get("ctr"))
        cpc = to_number(row.get("cpc"))
        cr = to_number(row.get("cr"))
        avg_pos = to_number(row.get("avg_position"))
        
        # Суммы
        day["views_sum"] += views
        day["clicks_sum"] += clicks
        day["adexp_sum"] += adexp
        day["atbs_sum"] += atbs
        day["orders_sum_cnt"] += orders_cnt
        day["shks_sum"] += shks
        day["orders_money_sum"] += orders_money
        
        # Для средних
        day["ctr_sum"] += ctr
        day["cpc_sum"] += cpc
        day["cr_sum"] += cr
        day["avg_pos_sum"] += avg_pos
        day["rows_cnt"] += 1
    
    # Формируем итоговые записи
    aggregated = []
    
    for group_key, group in groups.items():
        seller_id = group["seller_id"]
        advert_id = group["advert_id"]
        year = group["year"]
        month = group["month"]
        nmid = group["nmid"]
        
        seller_adid_month_year_key = f"{seller_id}_{advert_id}_{str(month).zfill(2)}_{year}"
        
        day_keys = sorted(group["days"].keys())  # сортировка по дате
        day_adstats = []
        
        for date_str in day_keys:
            d = group["days"][date_str]
            rows_cnt = d["rows_cnt"] or 1
            
            day_adstats.append({
                "date": date_str,
                "views": d["views_sum"],  # В JS без int()
                "clicks": d["clicks_sum"],  # В JS без int()
                "ctr": round_value(d["ctr_sum"] / rows_cnt, 3),
                "cpc": round_value(d["cpc_sum"] / rows_cnt, 3),
                "adexp": round_value(d["adexp_sum"], 3),
                "atbs": d["atbs_sum"],  # В JS без int()
                "orders_cnt": d["orders_sum_cnt"],  # В JS без int()
                "cr": round_value(d["cr_sum"] / rows_cnt, 3),
                "shks": d["shks_sum"],  # В JS без int()
                "orders_sum": round_value(d["orders_money_sum"], 3),
                "avg_pos": round_value(d["avg_pos_sum"] / rows_cnt, 3),
            })
        
        aggregated.append({
            "seller_adid_month_year_key": seller_adid_month_year_key,
            "month": month,
            "year": year,
            "seller_id": seller_id,
            "nmid": nmid,
            "advert_id": advert_id,
            "day_adstats": day_adstats,
        })
    
    if skipped_count > 0:
        print(f"[WB-bot] TOTAL: aggregate_ad_stats_rows - пропущено {skipped_count} строк из {len(ad_stats_rows)} (нет seller_id/advert_id/nmID)", flush=True)
    
    print(f"[WB-bot] TOTAL: aggregate_ad_stats_rows - входных строк: {len(ad_stats_rows)}, групп: {len(groups)}, выходных записей: {len(aggregated)}", flush=True)
    
    return aggregated


def write_stock_history_csv(rows: List[Dict], path: str) -> None:
    """Записывает агрегированные stock_history данные в CSV с Base64 кодированием JSON"""
    import csv
    from pathlib import Path

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "seller_nmid_month_year_key",
        "seller_id",
        "nmid",
        "year",
        "month",
        "stock_history",
    ]

    with target.open("w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            # Кодируем stock_history в Base64, как для warehouses в STOCKS
            stock_history = row.get("stock_history") or []
            stock_history_base64 = None
            if stock_history:
                try:
                    stock_history_json = json.dumps(stock_history, ensure_ascii=False, separators=(',', ':'))
                    json.loads(stock_history_json)  # Проверка валидности
                    stock_history_base64 = base64.b64encode(stock_history_json.encode("utf-8")).decode("ascii")
                except (TypeError, ValueError, json.JSONDecodeError):
                    stock_history_base64 = None
            
            writer.writerow(
                {
                    "seller_nmid_month_year_key": row.get("seller_nmid_month_year_key"),
                    "seller_id": row.get("seller_id"),
                    "nmid": row.get("nmid"),
                    "year": row.get("year"),
                    "month": row.get("month"),
                    "stock_history": stock_history_base64,
                }
            )


def load_stock_history_into_db(cursor: mysql.connector.cursor.MySQLCursor) -> None:
    """
    UPSERT через временную таблицу для обновления stock_history:
    1. Создаём временную таблицу
    2. LOAD DATA INFILE во временную таблицу (быстро)
    3. UPDATE из временной таблицы в целевую (обновляем только stock_history)
    """
    temp_table = "GS_RNP_ReportDetail_daily_nmid_stock_history_temp"
    
    try:
        # 1. Создаём временную таблицу (только нужные поля)
        cursor.execute(f"""
            CREATE TEMPORARY TABLE {temp_table} (
              seller_nmid_month_year_key VARCHAR(255) PRIMARY KEY,
              seller_id VARCHAR(255),
              nmid BIGINT,
              year INT,
              month INT,
              stock_history JSON
            )
        """)
        
        # 2. Загружаем данные во временную таблицу через LOAD DATA INFILE
        cursor.execute(f"""
            LOAD DATA INFILE '{TOTAL_NMID_STOCK_HISTORY_CSV_MYSQL_PATH}'
            INTO TABLE {temp_table}
            CHARACTER SET utf8mb4
            FIELDS TERMINATED BY ','
            ENCLOSED BY '"'
            LINES TERMINATED BY '\\n'
            IGNORE 1 LINES
            (
              seller_nmid_month_year_key,
              @seller_id,
              @nmid,
              @year,
              @month,
              @stock_history
            )
            SET
              seller_id = NULLIF(@seller_id, ''),
              nmid = NULLIF(@nmid, ''),
              year = NULLIF(@year, ''),
              month = NULLIF(@month, ''),
              stock_history = CASE
                WHEN @stock_history IS NULL OR @stock_history = '' THEN JSON_ARRAY()
                WHEN JSON_VALID(CONVERT(FROM_BASE64(@stock_history) USING utf8mb4)) THEN CAST(CONVERT(FROM_BASE64(@stock_history) USING utf8mb4) AS JSON)
                WHEN JSON_VALID(@stock_history) THEN CAST(@stock_history AS JSON)
                WHEN JSON_VALID(REPLACE(@stock_history, '""', '"')) THEN CAST(REPLACE(@stock_history, '""', '"') AS JSON)
                ELSE JSON_ARRAY()
              END
        """)
        
        # 3. UPDATE из временной таблицы в целевую (обновляем только stock_history)
        cursor.execute(f"""
            UPDATE GS_RNP_ReportDetail_daily_nmid AS r
            INNER JOIN {temp_table} AS t
              ON r.seller_nmid_month_year_key = t.seller_nmid_month_year_key
            SET r.stock_history = t.stock_history
        """)
        
        # 4. Временная таблица удалится автоматически при закрытии соединения
        
    except Error as exc:
        raise RuntimeError(f"UPSERT via temp table for stock_history failed: {exc}") from exc


def write_total_ad_stats_csv(rows: List[Dict], path: str) -> None:
    """Записывает агрегированные ad_stats данные для TOTAL воркфлоу в CSV с Base64 кодированием JSON"""
    import csv
    from pathlib import Path

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)

    # Порядок полей должен соответствовать порядку в таблице (без id):
    # seller_adid_month_year_key, month, year, seller_id, nmid, advert_id, day_adstats
    fieldnames = [
        "seller_adid_month_year_key",
        "month",
        "year",
        "seller_id",
        "nmid",
        "advert_id",
        "day_adstats",
    ]

    with target.open("w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            # Кодируем day_adstats в Base64, как для warehouses в STOCKS
            day_adstats = row.get("day_adstats") or []
            day_adstats_base64 = ""
            if day_adstats:
                try:
                    day_adstats_json = json.dumps(day_adstats, ensure_ascii=False, separators=(',', ':'))
                    json.loads(day_adstats_json)  # Проверка валидности
                    day_adstats_base64 = base64.b64encode(day_adstats_json.encode("utf-8")).decode("ascii")
                except (TypeError, ValueError, json.JSONDecodeError):
                    day_adstats_base64 = ""
            
            writer.writerow(
                {
                    "seller_adid_month_year_key": row.get("seller_adid_month_year_key"),
                    "seller_id": row.get("seller_id"),
                    "advert_id": row.get("advert_id"),
                    "nmid": row.get("nmid"),
                    "year": row.get("year"),
                    "month": row.get("month"),
                    "day_adstats": day_adstats_base64 or "",
                }
            )


def load_total_ad_stats_into_db(cursor: mysql.connector.cursor.MySQLCursor) -> None:
    """
    UPSERT через временную таблицу для обновления WB_ad_stats_daily (TOTAL воркфлоу):
    1. Создаём временную таблицу
    2. LOAD DATA INFILE во временную таблицу (быстро)
    3. INSERT ... ON DUPLICATE KEY UPDATE из временной в целевую
    """
    temp_table = "WB_ad_stats_daily_temp"
    
    try:
        # 1. Создаём временную таблицу (копия структуры целевой)
        cursor.execute(f"""
            CREATE TEMPORARY TABLE {temp_table} LIKE WB_ad_stats_daily
        """)
        
        # 2. Загружаем данные во временную таблицу через LOAD DATA INFILE
        load_start = time.time()
        print(f"[WB-bot] TOTAL: загружаем CSV {TOTAL_NMID_AD_STATS_CSV_MYSQL_PATH} во временную таблицу...", flush=True)
        cursor.execute(f"""
            LOAD DATA INFILE '{TOTAL_NMID_AD_STATS_CSV_MYSQL_PATH}'
            INTO TABLE {temp_table}
            CHARACTER SET utf8mb4
            FIELDS TERMINATED BY ','
            ENCLOSED BY '"'
            LINES TERMINATED BY '\\n'
            IGNORE 1 LINES
            (
              @seller_adid_month_year_key,
              @month,
              @year,
              @seller_id,
              @nmid,
              @advert_id,
              @day_adstats
            )
            SET
              seller_adid_month_year_key = NULLIF(@seller_adid_month_year_key, ''),
              month = NULLIF(@month, ''),
              year = NULLIF(@year, ''),
              seller_id = NULLIF(@seller_id, ''),
              nmid = NULLIF(@nmid, ''),
              advert_id = NULLIF(@advert_id, ''),
              day_adstats = CASE
                WHEN @day_adstats IS NULL OR @day_adstats = '' THEN JSON_ARRAY()
                -- Новая схема: day_adstats сохраняем в CSV в base64, чтобы избежать проблем с кавычками и разделителями
                -- Сначала декодируем Base64 в строку, затем проверяем и кастуем в JSON
                WHEN JSON_VALID(CONVERT(FROM_BASE64(@day_adstats) USING utf8mb4)) 
                  THEN CAST(CONVERT(FROM_BASE64(@day_adstats) USING utf8mb4) AS JSON)
                -- Прямая проверка валидности JSON (если уже правильно сериализован через json.dumps)
                WHEN JSON_VALID(@day_adstats) THEN CAST(@day_adstats AS JSON)
                -- Обработка экранированных кавычек из CSV: CSV writer удваивает кавычки внутри полей
                WHEN JSON_VALID(REPLACE(@day_adstats, '""', '"')) THEN CAST(REPLACE(@day_adstats, '""', '"') AS JSON)
                ELSE JSON_ARRAY()
              END
        """)
        load_elapsed = time.time() - load_start
        print(f"[WB-bot] TOTAL: LOAD DATA INFILE во временную таблицу занял {load_elapsed:.2f} сек", flush=True)
        
        # 3. INSERT ... ON DUPLICATE KEY UPDATE из временной таблицы в целевую
        # Используем seller_adid_month_year_key как уникальный ключ
        replace_start = time.time()
        print(f"[WB-bot] TOTAL: выполняем INSERT ... ON DUPLICATE KEY UPDATE из временной таблицы в WB_ad_stats_daily...", flush=True)
        cursor.execute(f"""
            INSERT INTO WB_ad_stats_daily
            (
              seller_adid_month_year_key,
              month,
              year,
              seller_id,
              nmid,
              advert_id,
              day_adstats
            )
            SELECT
              seller_adid_month_year_key,
              month,
              year,
              seller_id,
              nmid,
              advert_id,
              day_adstats
            FROM {temp_table}
            ON DUPLICATE KEY UPDATE
              seller_id = VALUES(seller_id),
              advert_id = VALUES(advert_id),
              nmid = VALUES(nmid),
              year = VALUES(year),
              month = VALUES(month),
              day_adstats = VALUES(day_adstats)
        """)
        replace_elapsed = time.time() - replace_start
        affected = cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0
        print(f"[WB-bot] TOTAL: INSERT ... ON DUPLICATE KEY UPDATE завершён за {replace_elapsed:.2f} сек, затронуто строк: {affected}", flush=True)
        
        # 4. Временная таблица удалится автоматически при закрытии соединения
        
    except Error as exc:
        raise RuntimeError(f"UPSERT via temp table for ad_stats failed: {exc}") from exc


def write_total_nmid_csv(rows: List[Dict], path: str) -> None:
    import csv
    from pathlib import Path

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "seller_nmid_month_year_key",
        "seller_id",
        "wb_api_brand",
        "nmid",
        "month",
        "year",
        "title",
        "photo",
        "Subject",
        "supplierArticle",
        "day_metrics",
    ]

    with target.open("w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            # Кодируем day_metrics в Base64, как для warehouses в STOCKS
            day_metrics = row.get("day_metrics") or []
            day_metrics_base64 = None
            if day_metrics:
                try:
                    day_metrics_json = json.dumps(day_metrics, ensure_ascii=False, separators=(',', ':'))
                    json.loads(day_metrics_json)  # Проверка валидности
                    day_metrics_base64 = base64.b64encode(day_metrics_json.encode("utf-8")).decode("ascii")
                except (TypeError, ValueError, json.JSONDecodeError):
                    day_metrics_base64 = None
            
            writer.writerow(
                {
                    "seller_nmid_month_year_key": row.get("seller_nmid_month_year_key"),
                    "seller_id": row.get("seller_id"),
                    "wb_api_brand": row.get("wb_api_brand"),
                    "nmid": row.get("nmid"),
                    "month": row.get("month"),
                    "year": row.get("year"),
                    "title": row.get("title"),
                    "photo": row.get("photo"),
                    "Subject": row.get("Subject"),
                    "supplierArticle": row.get("supplierArticle"),
                    "day_metrics": day_metrics_base64,
                }
            )


def load_total_nmid_into_db(cursor: mysql.connector.cursor.MySQLCursor) -> None:
    """
    UPSERT через временную таблицу для сохранения stock_history и stock_today:
    1. Создаём временную таблицу
    2. LOAD DATA INFILE во временную таблицу (быстро)
    3. INSERT ... ON DUPLICATE KEY UPDATE из временной в целевую
       (обновляем только day_metrics и метаданные, сохраняем stock_history и stock_today)
    """
    temp_table = "GS_RNP_ReportDetail_daily_nmid_temp"
    
    try:
        # 1. Создаём временную таблицу (копия структуры целевой)
        cursor.execute(f"""
            CREATE TEMPORARY TABLE {temp_table} LIKE GS_RNP_ReportDetail_daily_nmid
        """)
        
        # 2. Загружаем данные во временную таблицу через LOAD DATA INFILE (быстро!)
        cursor.execute(f"""
            LOAD DATA INFILE '{TOTAL_NMID_CSV_MYSQL_PATH}'
            INTO TABLE {temp_table}
            CHARACTER SET utf8mb4
            FIELDS TERMINATED BY ','
            ENCLOSED BY '"'
            LINES TERMINATED BY '\\n'
            IGNORE 1 LINES
            (
              seller_nmid_month_year_key,
              @seller_id,
              @wb_api_brand,
              @nmid,
              @month,
              @year,
              @title,
              @photo,
              @Subject,
              @supplierArticle,
              @day_metrics
            )
            SET
              seller_id = NULLIF(@seller_id, ''),
              wb_api_brand = NULLIF(@wb_api_brand, ''),
              nmid = NULLIF(@nmid, ''),
              month = NULLIF(@month, ''),
              year = NULLIF(@year, ''),
              title = NULLIF(@title, ''),
              photo = NULLIF(@photo, ''),
              Subject = NULLIF(@Subject, ''),
              supplierArticle = NULLIF(@supplierArticle, ''),
              day_metrics = CASE
                WHEN @day_metrics IS NULL OR @day_metrics = '' THEN JSON_ARRAY()
                -- Новая схема: day_metrics сохраняем в CSV в base64, чтобы избежать проблем с кавычками и разделителями
                WHEN JSON_VALID(CONVERT(FROM_BASE64(@day_metrics) USING utf8mb4)) THEN CAST(CONVERT(FROM_BASE64(@day_metrics) USING utf8mb4) AS JSON)
                -- Прямая проверка валидности JSON (если уже правильно сериализован через json.dumps)
                WHEN JSON_VALID(@day_metrics) THEN CAST(@day_metrics AS JSON)
                -- Обработка экранированных кавычек из CSV: CSV writer удваивает кавычки внутри полей
                WHEN JSON_VALID(REPLACE(@day_metrics, '""', '"')) THEN CAST(REPLACE(@day_metrics, '""', '"') AS JSON)
                ELSE JSON_ARRAY()
              END
        """)
        
        # 3. UPSERT из временной таблицы в целевую
        # Обновляем только day_metrics и метаданные, сохраняем stock_history и stock_today
        cursor.execute(f"""
            INSERT INTO GS_RNP_ReportDetail_daily_nmid
            SELECT * FROM {temp_table}
            ON DUPLICATE KEY UPDATE
              seller_id = VALUES(seller_id),
              wb_api_brand = VALUES(wb_api_brand),
              nmid = VALUES(nmid),
              month = VALUES(month),
              year = VALUES(year),
              title = VALUES(title),
              photo = VALUES(photo),
              Subject = VALUES(Subject),
              supplierArticle = VALUES(supplierArticle),
              day_metrics = VALUES(day_metrics)
              -- stock_history и stock_today НЕ обновляем - они заполняются в следующих циклах
        """)
        
        # 4. Временная таблица удалится автоматически при закрытии соединения
        
    except Error as exc:
        raise RuntimeError(f"UPSERT via temp table for total nmid failed: {exc}") from exc


def update_stock_history_for_card(
    cursor: mysql.connector.cursor.MySQLCursor,
    seller_id: str,
    nmid: Any,
) -> int:
    """Обновляет stock_history для одного артикула (seller_id + nmid)"""
    try:
        cursor.execute(
            TOTAL_NMID_STOCK_HISTORY_SQL,
            (
                seller_id, nmid,  # для SELECT в JOIN
                seller_id, nmid,  # для WHERE в подзапросе WB_storage_fee
                seller_id, nmid,  # для WHERE в подзапросе месяцев из GS_RNP_ReportDetail_daily_nmid
                seller_id, nmid,  # для WHERE в подзапросе DISTINCT месяцев из WB_storage_fee
                seller_id, nmid,  # для WHERE в UPDATE
            ),
        )
        return cursor.rowcount
    except Error as exc:
        raise RuntimeError(f"UPDATE stock_history failed for seller_id={seller_id}, nmid={nmid}: {exc}") from exc


def update_stock_today_for_card(
    cursor: mysql.connector.cursor.MySQLCursor,
    seller_id: str,
    nmid: Any,
) -> int:
    """Обновляет stock_today для одного артикула (seller_id + nmid)"""
    try:
        cursor.execute(
            TOTAL_NMID_STOCK_TODAY_SQL,
            (
                seller_id, nmid,  # для WHERE в подзапросе WB_stocks
                seller_id, nmid,  # для WHERE в UPDATE
            ),
        )
        return cursor.rowcount
    except Error as exc:
        raise RuntimeError(f"UPDATE stock_today failed for seller_id={seller_id}, nmid={nmid}: {exc}") from exc


def update_funnel_metrics_for_card(
    cursor: mysql.connector.cursor.MySQLCursor,
    seller_id: str,
    nmid: Any,
) -> int:
    """Обновляет funnel_metrics для одного артикула (seller_id + nmid)"""
    try:
        cursor.execute(
            TOTAL_NMID_FUNNEL_METRICS_SQL,
            (
                seller_id, nmid,  # для WHERE в подзапросе WB_analytics (a2)
                seller_id, nmid,  # для WHERE в основном запросе WB_analytics (a)
                seller_id, nmid,  # для WHERE в UPDATE
            ),
        )
        return cursor.rowcount
    except Error as exc:
        raise RuntimeError(f"UPDATE funnel_metrics failed for seller_id={seller_id}, nmid={nmid}: {exc}") from exc


def check_stocks_status(task_id: str, token: str) -> Optional[str]:
    """Проверяет статус отчета. Возвращает 'done', 'processing', 'error' или None"""
    url = WB_STOCKS_STATUS_URL.format(task_id=task_id)
    headers = {
        "user-agent": WB_USER_AGENT,
        "Authorization": f"Bearer {token}",
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=40)
        if response.status_code == 200:
            data = response.json()
            if isinstance(data, dict) and "data" in data:
                status = data["data"].get("status")
                if status:
                    return str(status).strip().lower()
        return None
    except Exception:
        return None


def download_stocks_data(task_id: str, token: str) -> Optional[List[Dict]]:
    """Скачивает данные отчета по остаткам"""
    url = WB_STOCKS_DOWNLOAD_URL.format(task_id=task_id)
    headers = {
        "user-agent": WB_USER_AGENT,
        "Authorization": f"Bearer {token}",
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=40)
        if response.status_code == 200:
            data = response.json()
            if isinstance(data, list):
                return data
        return None
    except Exception:
        return None


def process_stocks_response(response_data: List[Dict], seller_id: str) -> List[Dict]:
    """Обрабатывает ответ от WB API warehouse_remains и извлекает данные по складам"""
    result = []
    
    for item in response_data:
        # Фильтруем записи без barcode
        barcode = str(item.get("barcode", "")).strip()
        if not barcode:
            continue
        
        # Извлекаем данные из warehouses
        warehouses = item.get("warehouses", [])
        if not isinstance(warehouses, list):
            warehouses = []
        
        def find_quantity(warehouse_name: str) -> int:
            for wh in warehouses:
                if isinstance(wh, dict):
                    wh_name = str(wh.get("warehouseName", "")).strip()
                    if wh_name == warehouse_name:
                        qty = wh.get("quantity", 0)
                        return int(qty) if qty is not None else 0
            return 0
        
        quantity = find_quantity("Всего находится на складах")
        in_way_to_client = find_quantity("В пути до получателей")
        in_way_from_client = find_quantity("В пути возвраты на склад WB")
        quantity_full = quantity + in_way_to_client + in_way_from_client
        
        # Формируем seller_bc_key
        seller_bc_key = f"{seller_id}_{barcode}"
        
        # Формируем JSON для warehouses (компактный, без переносов строк)
        # Используем JSON.stringify аналогично n8n для правильной сериализации
        warehouses_json = None
        warehouses_base64 = None
        if warehouses:
            try:
                # Компактный JSON без пробелов и переносов строк для избежания проблем в CSV
                # ensure_ascii=False сохраняет кириллицу, separators убирает пробелы
                warehouses_json = json.dumps(warehouses, ensure_ascii=False, separators=(',', ':'))
                # Убеждаемся, что это валидная JSON строка (аналогично JSON.stringify в n8n)
                # Проверяем валидность через повторный парсинг
                json.loads(warehouses_json)
                warehouses_base64 = base64.b64encode(warehouses_json.encode("utf-8")).decode("ascii")
            except (TypeError, ValueError, json.JSONDecodeError):
                warehouses_json = None
                warehouses_base64 = None
        
        # Формируем строку для записи (порядок соответствует STOCKS_CSV_COLUMNS)
        row = {
            "seller_bc_key": seller_bc_key,
            "seller_id": seller_id,
            "barcode": barcode,
            "subjectName": str(item.get("subjectName", "")).strip() or None,
            "vendorCode": str(item.get("vendorCode", "")).strip() or None,
            "nmId": int(item.get("nmId", 0) or 0) if item.get("nmId") else None,
            "techSize": str(item.get("techSize", "")).strip() or None,
            "volume": float(item.get("volume", 0) or 0) if item.get("volume") is not None else None,
            "quantity": quantity,
            "inWayToClient": in_way_to_client,
            "inWayFromClient": in_way_from_client,
            "quantityFull": quantity_full,
            "warehouses": warehouses_base64,
        }
        
        result.append(row)
    
    return result


def write_stocks_csv(rows: List[Dict], path: str) -> None:
    import csv
    from pathlib import Path

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)

    with target.open("w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=STOCKS_CSV_COLUMNS)
        writer.writeheader()
        for chunk_start in range(0, len(rows), 40000):
            chunk = rows[chunk_start : chunk_start + 40000]
            writer.writerows(chunk)


def build_stocks_selection_message(summary_text: str) -> str:
    escaped = html.escape(summary_text)
    return (
        "<b>07 WB API</b> | STOCKS\n"
        "<blockquote>Подготовка выгрузки остатков.\n"
        f"<code>{escaped}</code></blockquote>"
    )


def fetch_products_sellers(cursor: mysql.connector.cursor.MySQLCursorDict) -> List[Dict]:
    cursor.execute(
        """
        SELECT
            s.seller_id,
            s.wb_api_key,
            s.wb_api_brand,
            su.products_status
        FROM WB_sellers AS s
        JOIN WB_sellers_updates AS su
            ON su.seller_id = s.seller_id
        WHERE su.in_workrnp = 1
        """
    )
    rows = cursor.fetchall()
    result: List[Dict] = []
    for row in rows:
        status = row.get("products_status")
        if isinstance(status, str):
            try:
                row["products_status"] = json.loads(status)
            except json.JSONDecodeError:
                row["products_status"] = None
        result.append(row)
    return result


def compute_products_priority(
    sellers: Iterable[Dict],
) -> Tuple[List[Dict], List[Dict], str]:
    processed: List[Dict] = []
    per_seller: Dict[str, Dict] = {}

    for idx, raw in enumerate(sellers):
        seller_id = str(raw.get("seller_id") or "").strip()
        if not seller_id:
            continue

        products_status = raw.get("products_status") or None
        brand = (raw.get("wb_api_brand") or seller_id).strip()

        status_cat = "new" if not products_status else "existing"
        now_time = products_status.get("nowTime") if products_status else None

        bucket = per_seller.get(seller_id)
        if bucket is None:
            bucket = {
                "seller_id": seller_id,
                "brand": brand or seller_id,
                "status_cat": status_cat,
                "idx": idx,
                "products_status": products_status,
                "now_time": now_time,
                "cooldown_blocked": False,
            }
            per_seller[seller_id] = bucket
        else:
            if bucket["status_cat"] != "new" and status_cat == "new":
                bucket["status_cat"] = status_cat
            if not bucket.get("brand"):
                bucket["brand"] = brand
            if not bucket.get("now_time") and now_time:
                bucket["now_time"] = now_time

        processed.append(
            {
                "seller_id": seller_id,
                "brand": brand or seller_id,
                "status_cat": status_cat,
                "idx": idx,
                "wb_api_key": raw.get("wb_api_key"),
                "products_status": products_status,
                "now_time": now_time,
                "cooldown_blocked": False,
            }
        )

    all_sellers = list(per_seller.values())

    new_group = [s for s in all_sellers if s["status_cat"] == "new"]
    existing_group = [s for s in all_sellers if s["status_cat"] != "new"]

    allow_set = set()

    if new_group:
        allow_set.update(s["seller_id"] for s in new_group)
    else:
        eligible = []
        for s in existing_group:
            status_dict = s.get("products_status") or {}
            now_dt = parse_msk_datetime(status_dict.get("nowTime"))
            if now_dt is None:
                eligible.append(s)
                continue
            age_min = minutes_since_msk(now_dt)
            if age_min >= PRODUCTS_COOLDOWN_MINUTES:
                eligible.append(s)
            else:
                s["cooldown_blocked"] = True
        if eligible:
            allow_set.update(s["seller_id"] for s in eligible)

    lines = []
    for s in sorted(all_sellers, key=lambda item: item["brand"].lower()):
        allowed = s["seller_id"] in allow_set
        mark = "✅" if allowed else "✖️"
        status_dict = s.get("products_status") or {}
        now_dt = parse_msk_datetime(status_dict.get("nowTime") or s.get("now_time"))
        if s["status_cat"] == "new":
            minutes_text = "new"
        elif now_dt is not None:
            age_min = max(0, int(minutes_since_msk(now_dt)))
            minutes_text = f"{age_min}min"
        else:
            minutes_text = "--"
        lines.append(f"{minutes_text} | {s['brand']} {mark}")

    summary_text = "\n".join(lines)

    allowed_items: List[Dict] = []
    for item in processed:
        if item["seller_id"] in allow_set:
            enriched = dict(item)
            enriched["text"] = summary_text
            allowed_items.append(enriched)

    return allowed_items, all_sellers, summary_text


def build_products_selection_message(summary_text: str) -> str:
    escaped = html.escape(summary_text)
    return (
        "<b>03 WB API</b> | Products\n"
        "<blockquote>Подготовка выгрузки карточек.\n"
        f"<code>{escaped}</code></blockquote>"
    )


def _parse_now_time(now_time: Optional[str]) -> Tuple[int, str]:
    if not now_time:
        return (1, "")
    try:
        dt = datetime.strptime(now_time[:19], "%Y-%m-%d %H:%M:%S")
        return (0, dt.isoformat(sep=" "))
    except ValueError:
        try:
            dt = datetime.strptime(now_time[:19], "%Y-%m-%dT%H:%M:%S")
            return (0, dt.isoformat(sep=" "))
        except ValueError:
            return (1, now_time)


def parse_msk_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            dt = datetime.strptime(value[:19], fmt)
            return dt.replace(tzinfo=MSK_OFFSET)
        except ValueError:
            continue
    return None


def minutes_since_msk(dt: datetime) -> float:
    diff = msk_now() - dt
    return diff.total_seconds() / 60


def right_status_is_ready(status_dict: Optional[Dict]) -> bool:
    if not status_dict:
        return True
    now_time_str = status_dict.get("nowTime")
    dt = parse_msk_datetime(now_time_str)
    if dt is None:
        return True
    age_min = minutes_since_msk(dt)
    return age_min >= RIGHT_COOLDOWN_MINUTES


def right_status_cooldown_minutes(status_dict: Optional[Dict]) -> float:
    if not status_dict:
        return 0.0
    now_time_str = status_dict.get("nowTime")
    dt = parse_msk_datetime(now_time_str)
    if dt is None:
        return 0.0
    age_min = minutes_since_msk(dt)
    remaining = RIGHT_COOLDOWN_MINUTES - age_min
    return max(0.0, remaining)


def compute_date_from_and_priority(
    sellers: Iterable[Dict],
) -> Tuple[List[Dict], List[Dict], str]:
    processed: List[Dict] = []
    per_seller: Dict[str, Dict] = {}
    threshold180 = ymd_minus_days(180)
    default_date_from = ymd_minus_days(15)

    for idx, raw in enumerate(sellers):
        seller_id = str(raw.get("seller_id") or "").strip()
        if not seller_id:
            continue

        orders_status = raw.get("orders_status") or None
        brand = (raw.get("wb_api_brand") or seller_id).strip()

        status_cat: str
        date_from: str

        now_time: Optional[str] = None

        if not orders_status:
            status_cat = "new"
            date_from = default_date_from
        else:
            status_raw = orders_status.get("status")
            rb_ymd = first_ymd(orders_status.get("rightBoundary"))
            last_df = first_ymd(orders_status.get("lastDateFrom"))
            max_df = first_ymd(orders_status.get("maxDateFrom"))
            now_time = orders_status.get("nowTime")

            if status_raw == "right":
                candidate = sub_days_from_ymd(rb_ymd, 15) if rb_ymd else None
                date_from = candidate or last_df or max_df or default_date_from
            elif status_raw == "left":
                if max_df and max_df > threshold180:
                    date_from = threshold180
                elif max_df and max_df <= threshold180:
                    date_from = rb_ymd or threshold180 or default_date_from
                else:
                    date_from = last_df or max_df or default_date_from
            else:
                date_from = last_df or max_df or default_date_from

            if status_raw == "left":
                status_cat = "left"
            elif status_raw == "right":
                status_cat = "right"
            else:
                status_cat = "other"

        bucket = per_seller.get(seller_id)
        if bucket is None:
            bucket = {
                "seller_id": seller_id,
                "brand": brand or seller_id,
                "status_cat": status_cat,
                "date_from": date_from,
                "idx": idx,
                "orders_status": orders_status,
                "now_time": now_time,
                "cooldown_blocked": False,
            }
            per_seller[seller_id] = bucket
        else:
            rank = {"other": 0, "right": 1, "left": 2, "new": 3}
            if rank[status_cat] > rank[bucket["status_cat"]]:
                bucket["status_cat"] = status_cat
            if not bucket.get("date_from"):
                bucket["date_from"] = date_from
            if not bucket.get("brand"):
                bucket["brand"] = brand
            if not bucket.get("now_time") and now_time:
                bucket["now_time"] = now_time
            if "cooldown_blocked" not in bucket:
                bucket["cooldown_blocked"] = False

        processed.append(
            {
                "seller_id": seller_id,
                "brand": brand or seller_id,
                "status_cat": status_cat,
                "date_from": date_from,
                "idx": idx,
                "wb_api_key": raw.get("wb_api_key"),
                "orders_status": orders_status,
            }
        )

    all_sellers = list(per_seller.values())

    new_group = [s for s in all_sellers if s["status_cat"] == "new"]
    left_group = [s for s in all_sellers if s["status_cat"] == "left"]
    right_group = [s for s in all_sellers if s["status_cat"] == "right"]

    allow_set = set()

    if new_group:
        allow_set.update(s["seller_id"] for s in new_group)
    elif left_group:
        left_group.sort(key=lambda s: _parse_now_time((s.get("orders_status") or {}).get("nowTime")))
        allow_set.add(left_group[0]["seller_id"])
    elif right_group and len(right_group) == len(all_sellers):
        eligible_rights = []
        for s in right_group:
            status_dict = s.get("orders_status") or {}
            if right_status_is_ready(status_dict):
                eligible_rights.append(s)
            else:
                s["cooldown_blocked"] = True
        if eligible_rights:
            allow_set.update(s["seller_id"] for s in eligible_rights)

    for s in all_sellers:
        s["allowed"] = s["seller_id"] in allow_set

    lines = []
    for s in sorted(all_sellers, key=lambda item: item["brand"].lower()):
        mark = "✅" if s["allowed"] else "✖️"
        try:
            dt_obj = datetime.strptime(s["date_from"], "%Y-%m-%d")
            date_short = dt_obj.strftime("%d.%m.%y")
        except Exception:
            date_short = s["date_from"]
        now_dt = parse_msk_datetime((s.get("sales_status") or {}).get("nowTime") or s.get("now_time"))
        minutes_text = "--"
        if now_dt is not None:
            age_min = max(0, int(minutes_since_msk(now_dt)))
            minutes_text = f"{age_min}min"
        lines.append(f"{s['status_cat']} | dtFrm: {date_short} | {minutes_text} | {s['brand']} {mark}")
    summary_text = "\n".join(lines)

    allowed_items: List[Dict] = []
    for item in processed:
        if item["seller_id"] in allow_set:
            enriched = dict(item)
            enriched["text"] = summary_text
            allowed_items.append(enriched)

    return allowed_items, all_sellers, summary_text


def build_summary_message(summary_text: str) -> str:
    escaped = html.escape(summary_text)
    return (
        "<b>01 WB API</b> | Orders\n"
        f"<blockquote>{escaped}</blockquote>"
    )


def fetch_orders_for_seller(item: Dict) -> List[Dict]:
    params = {"dateFrom": item["date_from"]}
    headers = {
        "user-agent": WB_USER_AGENT,
        "Authorization": f"Bearer {item.get('wb_api_key', '').strip()}",
    }
    response = requests.get(WB_ORDERS_URL, params=params, headers=headers, timeout=30)
    if response.status_code != 200:
        raise RuntimeError(
            f"WB API error for {item['seller_id']}: {response.status_code} {response.text}"
        )
    data = response.json()
    if not isinstance(data, list):
        raise RuntimeError(
            f"WB API unexpected response for {item['seller_id']}: {data!r}"
        )
    enriched: List[Dict] = []
    for row in data:
        payload = dict(row)
        payload["seller_id"] = item["seller_id"]
        payload["wb_api_brand"] = item.get("brand")
        payload["dateFrom"] = item["date_from"]
        payload["wb_api_key"] = item.get("wb_api_key")
        payload["orders_status"] = item.get("orders_status")
        enriched.append(payload)
    return enriched


def build_orders_summary(items: List[Dict]) -> str:
    def first_ymd(value: Optional[str]) -> Optional[str]:
        if not value:
            return None
        value = str(value)
        match = value[:10]
        try:
            datetime.strptime(match, "%Y-%m-%d")
            return match
        except ValueError:
            return None

    def min_str(a: Optional[str], b: Optional[str]) -> Optional[str]:
        if a is None:
            return b
        if b is None:
            return a
        return a if a < b else b

    def max_str(a: Optional[str], b: Optional[str]) -> Optional[str]:
        if a is None:
            return b
        if b is None:
            return a
        return a if a > b else b

    def fmt_int(value: int) -> str:
        return f"{value:,}".replace(",", " ")

    per_seller: Dict[str, Dict[str, Optional[str]]] = {}
    for row in items:
        seller_id = str(row.get("seller_id") or "").strip() or "unknown"
        brand = (str(row.get("wb_api_brand") or "")).strip() or seller_id
        ymd_val = first_ymd(row.get("date")) or first_ymd(row.get("lastChangeDate"))

        bucket = per_seller.get(seller_id)
        if bucket is None:
            bucket = {"brand": brand, "min": None, "max": None, "cnt": 0}
            per_seller[seller_id] = bucket

        bucket["cnt"] = int(bucket.get("cnt") or 0) + 1
        bucket["brand"] = bucket.get("brand") or brand
        if ymd_val:
            bucket["min"] = min_str(bucket.get("min"), ymd_val)
            bucket["max"] = max_str(bucket.get("max"), ymd_val)

    lines = []
    for bucket in sorted(per_seller.values(), key=lambda b: b["brand"]):
        line = (
            f"{bucket.get('min') or '-'} · "
            f"{bucket.get('max') or '-'} · "
            f"{fmt_int(bucket.get('cnt') or 0)} · "
            f"{bucket.get('brand')}"
        )
        lines.append(line)

    return "\n".join(lines)


def to_mysql_datetime(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    if isinstance(value, str):
        if value.startswith("0001-01-01"):
            return None
        return value.replace("T", " ").replace("Z", "")
    return str(value)


CSV_COLUMNS = [
    "date",
    "lastChangeDate",
    "warehouseName",
    "warehouseType",
    "countryName",
    "oblastOkrugName",
    "regionName",
    "supplierArticle",
    "nmId",
    "barcode",
    "category",
    "subject",
    "brand",
    "techSize",
    "incomeID",
    "isSupply",
    "isRealization",
    "totalPrice",
    "discountPercent",
    "spp",
    "finishedPrice",
    "priceWithDisc",
    "isCancel",
    "cancelDate",
    "orderType",
    "sticker",
    "gNumber",
    "srid",
    "seller_id",
    "srid_seller_key",
]


def convert_orders_for_csv(items: Iterable[Dict]) -> List[Dict]:
    rows: List[Dict] = []
    for row in items:
        seller_id = row.get("seller_id") or row.get("sellerId")
        srid = row.get("srid")
        csv_row = {
            "date": to_mysql_datetime(row.get("date")),
            "lastChangeDate": to_mysql_datetime(row.get("lastChangeDate")),
            "warehouseName": row.get("warehouseName"),
            "warehouseType": row.get("warehouseType"),
            "countryName": row.get("countryName"),
            "oblastOkrugName": row.get("oblastOkrugName"),
            "regionName": row.get("regionName"),
            "supplierArticle": row.get("supplierArticle"),
            "nmId": row.get("nmId"),
            "barcode": row.get("barcode"),
            "category": row.get("category"),
            "subject": row.get("subject"),
            "brand": row.get("brand"),
            "techSize": row.get("techSize"),
            "incomeID": row.get("incomeID"),
            "isSupply": 1 if row.get("isSupply") else 0,
            "isRealization": 1 if row.get("isRealization") else 0,
            "totalPrice": row.get("totalPrice"),
            "discountPercent": row.get("discountPercent"),
            "spp": row.get("spp"),
            "finishedPrice": row.get("finishedPrice"),
            "priceWithDisc": row.get("priceWithDisc"),
            "isCancel": 1 if row.get("isCancel") else 0,
            "cancelDate": to_mysql_datetime(row.get("cancelDate")),
            "orderType": row.get("orderType"),
            "sticker": row.get("sticker"),
            "gNumber": row.get("gNumber"),
            "srid": srid,
            "seller_id": seller_id,
            "srid_seller_key": f"{srid}_{seller_id}" if srid and seller_id else None,
        }
        rows.append(csv_row)
    return rows


SALES_CSV_COLUMNS = [
    "date",
    "lastChangeDate",
    "warehouseName",
    "warehouseType",
    "countryName",
    "oblastOkrugName",
    "regionName",
    "supplierArticle",
    "nmId",
    "barcode",
    "category",
    "subject",
    "brand",
    "techSize",
    "incomeID",
    "isSupply",
    "isRealization",
    "totalPrice",
    "discountPercent",
    "spp",
    "paymentSaleAmount",
    "forPay",
    "finishedPrice",
    "priceWithDisc",
    "saleID",
    "orderType",
    "sticker",
    "gNumber",
    "srid",
    "seller_id",
    "srid_seller_sale_key",
]


def convert_sales_for_csv(items: Iterable[Dict]) -> List[Dict]:
    rows: List[Dict] = []
    for row in items:
        seller_id = row.get("seller_id") or row.get("sellerId")
        srid = row.get("srid")
        sale_id = row.get("saleID") or row.get("saleId")
        srid_seller_sale_key = (
            f"{srid}_{seller_id}_{sale_id}" if srid and seller_id and sale_id else None
        )
        csv_row = {
            "date": to_mysql_datetime(row.get("date")),
            "lastChangeDate": to_mysql_datetime(row.get("lastChangeDate")),
            "warehouseName": row.get("warehouseName"),
            "warehouseType": row.get("warehouseType"),
            "countryName": row.get("countryName"),
            "oblastOkrugName": row.get("oblastOkrugName"),
            "regionName": row.get("regionName"),
            "supplierArticle": row.get("supplierArticle"),
            "nmId": row.get("nmId"),
            "barcode": row.get("barcode"),
            "category": row.get("category"),
            "subject": row.get("subject"),
            "brand": row.get("brand"),
            "techSize": row.get("techSize"),
            "incomeID": row.get("incomeID"),
            "isSupply": 1 if row.get("isSupply") else 0,
            "isRealization": 1 if row.get("isRealization") else 0,
            "totalPrice": row.get("totalPrice"),
            "discountPercent": row.get("discountPercent"),
            "spp": row.get("spp"),
            "paymentSaleAmount": row.get("paymentSaleAmount"),
            "forPay": row.get("forPay"),
            "finishedPrice": row.get("finishedPrice"),
            "priceWithDisc": row.get("priceWithDisc"),
            "saleID": sale_id,
            "orderType": row.get("orderType"),
            "sticker": row.get("sticker"),
            "gNumber": row.get("gNumber"),
            "srid": srid,
            "seller_id": seller_id,
            "srid_seller_sale_key": srid_seller_sale_key,
        }
        rows.append(csv_row)
    return rows
def update_orders_status(
    cursor: mysql.connector.cursor.MySQLCursor,
    sellers_meta: Iterable[Dict],
    order_rows: Iterable[Dict],
    api_error_sellers: Set[str],
) -> int:
    threshold180 = ymd_minus_days(180)

    def min_ymd(a: Optional[str], b: Optional[str]) -> Optional[str]:
        if a is None:
            return b
        if b is None:
            return a
        return a if a < b else b

    def max_ymd(a: Optional[str], b: Optional[str]) -> Optional[str]:
        if a is None:
            return b
        if b is None:
            return a
        return a if a > b else b

    per_seller: Dict[str, Dict] = {}

    for meta in sellers_meta:
        seller_id = str(meta.get("seller_id") or "").strip()
        if not seller_id:
            continue
        per_seller[seller_id] = {
            "rows": [],
            "prev": meta.get("orders_status"),
            "date_from": first_ymd(meta.get("date_from") or meta.get("dateFrom")),
            "has_error": False,
            "api_error": seller_id in api_error_sellers,
            "brand": meta.get("brand") or meta.get("wb_api_brand") or seller_id,
        }

    for row in order_rows:
        seller_id = str(row.get("seller_id") or "").strip()
        if not seller_id:
            continue

        bucket = per_seller.setdefault(
            seller_id,
            {
                "rows": [],
                "prev": row.get("orders_status"),
                "date_from": first_ymd(row.get("dateFrom")),
                "has_error": False,
                "api_error": seller_id in api_error_sellers,
                "brand": row.get("wb_api_brand") or seller_id,
            },
        )

        looks_like_order = any(
            row.get(field)
            for field in ("date", "lastChangeDate", "srid", "nmId", "barcode", "gNumber")
        )
        looks_like_error = (
            not looks_like_order
            and not row.get("orders_status")
            and any(
                row.get(field)
                for field in (
                    "error",
                    "errorMessage",
                    "message",
                    "code",
                    "statusCode",
                    "wb_error",
                )
            )
        )

        if looks_like_error:
            bucket["has_error"] = True
        else:
            bucket["rows"].append(row)

        if not bucket["prev"] and row.get("orders_status"):
            bucket["prev"] = row["orders_status"]

        if not bucket["date_from"] and row.get("dateFrom"):
            bucket["date_from"] = first_ymd(row.get("dateFrom"))

    updates: List[Tuple[str, str]] = []
    details: List[Dict] = []
    now_time = msk_now().strftime("%Y-%m-%d %H:%M:%S")

    for seller_id, bucket in per_seller.items():
        rows = bucket["rows"]
        count = len(rows)
        prev = bucket["prev"]
        current_date_from = first_ymd(bucket.get("date_from"))
        if not current_date_from:
            current_date_from = first_ymd((prev or {}).get("lastDateFrom")) or first_ymd((prev or {}).get("maxDateFrom"))
        prev_max_date_from = first_ymd((prev or {}).get("maxDateFrom"))

        if bucket.get("api_error") or bucket["has_error"]:
            if prev:
                orders_status = prev
            else:
                continue
        elif count == 0:
            max_date_from = prev_max_date_from or current_date_from
            orders_status = {
                "maxDateFrom": max_date_from,
                "status": "right",
                "lastTotalOrders": 0,
                "leftBoundary": "",
                "rightBoundary": "",
                "lastDateFrom": current_date_from,
                "nowTime": now_time,
            }
            if max_date_from is None:
                orders_status["maxDateFrom"] = ""
            if orders_status["lastDateFrom"] is None:
                orders_status["lastDateFrom"] = ""
        else:
            left_boundary = None
            right_boundary = None
            for r in rows:
                ymd_val = first_ymd(r.get("date")) or first_ymd(r.get("lastChangeDate"))
                if ymd_val:
                    left_boundary = min_ymd(left_boundary, ymd_val)
                    right_boundary = max_ymd(right_boundary, ymd_val)

            prev_status = (prev or {}).get("status")

            if not prev:
                new_max_date_from = current_date_from or None
                new_status = "left"
            else:
                candidates = [
                    first_ymd(current_date_from),
                    prev_max_date_from,
                ]
                candidates = [c for c in candidates if c]
                new_max_date_from = min(candidates) if candidates else None

                count_ok = 0 < count < 80000
                cond_b = current_date_from and current_date_from <= threshold180 and count_ok
                cond_c = prev_max_date_from and prev_max_date_from <= threshold180 and count_ok

                if prev_status == "right" or cond_b or cond_c:
                    new_status = "right"
                else:
                    new_status = "left"

            orders_status = {
                "maxDateFrom": new_max_date_from,
                "status": new_status,
                "lastTotalOrders": count,
                "leftBoundary": left_boundary,
                "rightBoundary": right_boundary,
                "lastDateFrom": current_date_from,
                "nowTime": now_time,
            }

        updates.append((json.dumps(orders_status, ensure_ascii=False), seller_id))
        details.append(
            {
                "seller_id": seller_id,
                "brand": bucket.get("brand") or seller_id,
                "from": (prev or {}).get("status") if prev else "null",
                "to": orders_status.get("status") if isinstance(orders_status, dict) else (prev or {}).get("status"),
                "count": count,
                "date_from": current_date_from,
            }
        )

    if updates:
        cursor.executemany(
            """
            UPDATE WB_sellers_updates
            SET orders_status = %s
            WHERE seller_id = %s
            """,
            updates,
        )

    return len(updates), details


def write_orders_csv(rows: List[Dict], path: str) -> None:
    import csv
    from pathlib import Path

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)

    with target.open("w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        for chunk_start in range(0, len(rows), 40000):
            chunk = rows[chunk_start : chunk_start + 40000]
            writer.writerows(chunk)


def write_sales_csv(rows: List[Dict], path: str) -> None:
    import csv
    from pathlib import Path

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)

    with target.open("w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=SALES_CSV_COLUMNS)
        writer.writeheader()
        for chunk_start in range(0, len(rows), 40000):
            chunk = rows[chunk_start : chunk_start + 40000]
            writer.writerows(chunk)


def load_orders_into_db(cursor: mysql.connector.cursor.MySQLCursor) -> None:
    try:
        cursor.execute(
            f"""
            LOAD DATA INFILE '{WB_CSV_MYSQL_PATH}'
            REPLACE
            INTO TABLE WB_orders
            CHARACTER SET utf8mb4
            FIELDS TERMINATED BY ','
            ENCLOSED BY '"'
            LINES TERMINATED BY '\\n'
            IGNORE 1 LINES
            (
              date,
              lastChangeDate,
              warehouseName,
              warehouseType,
              countryName,
              oblastOkrugName,
              regionName,
              supplierArticle,
              nmId,
              barcode,
              category,
              subject,
              brand,
              techSize,
              incomeID,
              isSupply,
              isRealization,
              totalPrice,
              discountPercent,
              spp,
              finishedPrice,
              priceWithDisc,
              isCancel,
              cancelDate,
              orderType,
              sticker,
              gNumber,
              srid,
              seller_id,
              srid_seller_key
            )
            """
        )
    except Error as exc:
        raise RuntimeError(f"LOAD DATA INFILE failed: {exc}") from exc


def load_sales_into_db(cursor: mysql.connector.cursor.MySQLCursor) -> None:
    try:
        cursor.execute(
            f"""
            LOAD DATA INFILE '{WB_SALES_CSV_MYSQL_PATH}'
            REPLACE
            INTO TABLE WB_sales
            CHARACTER SET utf8mb4
            FIELDS TERMINATED BY ','
            ENCLOSED BY '"'
            LINES TERMINATED BY '\\n'
            IGNORE 1 LINES
            (
              date,
              lastChangeDate,
              warehouseName,
              warehouseType,
              countryName,
              oblastOkrugName,
              regionName,
              supplierArticle,
              nmId,
              barcode,
              category,
              subject,
              brand,
              techSize,
              incomeID,
              isSupply,
              isRealization,
              totalPrice,
              discountPercent,
              spp,
              paymentSaleAmount,
              forPay,
              finishedPrice,
              priceWithDisc,
              saleID,
              orderType,
              sticker,
              gNumber,
              srid,
              seller_id,
              srid_seller_sale_key
            )
            """
        )
    except Error as exc:
        raise RuntimeError(f"LOAD DATA INFILE for sales failed: {exc}") from exc


def load_products_into_db(cursor: mysql.connector.cursor.MySQLCursor) -> None:
    try:
        cursor.execute(
            f"""
            LOAD DATA INFILE '{WB_PRODUCTS_CSV_MYSQL_PATH}'
            REPLACE
            INTO TABLE WB_products
            CHARACTER SET utf8mb4
            FIELDS TERMINATED BY ','
            ENCLOSED BY '"'
            LINES TERMINATED BY '\\n'
            IGNORE 1 LINES
            (
              @seller_id,
              @nmID,
              @imtID,
              @vendorCode,
              @subjectID,
              @subjectName,
              @brand,
              @title,
              @description,
              @photo,
              @photo_high,
              @video,
              @dimensions_length,
              @dimensions_width,
              @dimensions_height,
              @weightBrutto,
              @dimensions_isValid,
              @characteristics,
              @tags,
              @skus,
              @techSize,
              @createdAt,
              @updatedAt,
              @needKiz
            )
            SET
              seller_id = NULLIF(@seller_id, ''),
              seller_skus_key = CASE WHEN @skus = '' THEN NULL ELSE CONCAT(@seller_id, '_', @skus) END,
              nmID = NULLIF(@nmID, ''),
              imtID = NULLIF(@imtID, ''),
              vendorCode = NULLIF(@vendorCode, ''),
              subjectName = NULLIF(@subjectName, ''),
              brand = NULLIF(@brand, ''),
              title = NULLIF(@title, ''),
              description = NULLIF(@description, ''),
              photo = NULLIF(@photo, ''),
              photo_high = NULLIF(@photo_high, ''),
              video = NULLIF(@video, ''),
              dimensions_length = NULLIF(@dimensions_length, ''),
              dimensions_width = NULLIF(@dimensions_width, ''),
              dimensions_height = NULLIF(@dimensions_height, ''),
              weightBrutto = NULLIF(@weightBrutto, ''),
              dimensions_isValid = IF(@dimensions_isValid = '' OR @dimensions_isValid IS NULL, 0, @dimensions_isValid),
              characteristics = CASE
                WHEN @characteristics = '' THEN JSON_ARRAY()
                WHEN JSON_VALID(@characteristics) THEN CAST(@characteristics AS JSON)
                ELSE JSON_ARRAY()
              END,
              size = NULLIF(@techSize, ''),
              tags = CASE
                WHEN @tags = '' THEN JSON_ARRAY()
                WHEN JSON_VALID(@tags) THEN CAST(@tags AS JSON)
                ELSE JSON_ARRAY()
              END,
              skus = NULLIF(@skus, ''),
              needKiz = IF(@needKiz = '' OR @needKiz IS NULL, 0, @needKiz),
              createdAt = NULLIF(@createdAt, ''),
              updatedAt = NULLIF(@updatedAt, '')
            """
        )
    except Error as exc:
        raise RuntimeError(f"LOAD DATA INFILE for products failed: {exc}") from exc


def load_ad_list_into_db(cursor: mysql.connector.cursor.MySQLCursor) -> None:
    try:
        cursor.execute(
            f"""
            LOAD DATA INFILE '{WB_AD_LIST_CSV_MYSQL_PATH}'
            REPLACE
            INTO TABLE WB_ad_campaigns
            CHARACTER SET utf8mb4
            FIELDS TERMINATED BY ','
            ENCLOSED BY '"'
            LINES TERMINATED BY '\\n'
            IGNORE 1 LINES
            (
              seller_id,
              advert_seller_key,
              advertId,
              type,
              status,
              @changeTime
            )
            SET
              changeTime = NULLIF(@changeTime, '')
            """
        )
    except Error as exc:
        raise RuntimeError(f"LOAD DATA INFILE for ad list failed: {exc}") from exc


def load_ad_stats_into_db(cursor: mysql.connector.cursor.MySQLCursor) -> None:
    """
    UPSERT через временную таблицу для обновления WB_ad_stats:
    1. Создаём временную таблицу
    2. LOAD DATA INFILE во временную таблицу (быстро)
    3. REPLACE INTO из временной в целевую с фильтрацией валидных seller_id
    """
    temp_table = "WB_ad_stats_temp"
    
    try:
        # 1. Создаём временную таблицу (копия структуры целевой, но без внешних ключей)
        cursor.execute(f"""
            CREATE TEMPORARY TABLE {temp_table} LIKE WB_ad_stats
        """)
        
        # 2. Загружаем данные во временную таблицу через LOAD DATA INFILE
        # Используем оригинальную логику: seller_advert_date_key и seller_id загружаются напрямую
        cursor.execute(
            f"""
            LOAD DATA INFILE '{WB_AD_STATS_CSV_MYSQL_PATH}'
            INTO TABLE {temp_table}
            CHARACTER SET utf8mb4
            FIELDS TERMINATED BY ','
            ENCLOSED BY '"'
            LINES TERMINATED BY '\\n'
            IGNORE 1 LINES
            (
              seller_advert_date_key,
              seller_id,
              @advertId,
              @date,
              @nmId,
              @appType,
              @views,
              @clicks,
              @ctr,
              @cpc,
              @ad_expenses,
              @atbs,
              @orders,
              @shks,
              @cr,
              @canceled,
              @sum,
              @sum_price,
              @avg_position
            )
            SET
              advert_id = NULLIF(@advertId, ''),
              date = NULLIF(@date, ''),
              nmID = NULLIF(@nmId, ''),
              views = NULLIF(@views, ''),
              clicks = NULLIF(@clicks, ''),
              ctr = NULLIF(@ctr, ''),
              cpc = NULLIF(@cpc, ''),
              ad_expenses = NULLIF(@ad_expenses, ''),
              atbs = NULLIF(@atbs, ''),
              orders = NULLIF(@orders, ''),
              canceled = NULLIF(@canceled, ''),
              cr = NULLIF(@cr, ''),
              shks = NULLIF(@shks, ''),
              sum_price = NULLIF(@sum_price, ''),
              app_type = NULLIF(@appType, ''),
              avg_position = NULLIF(@avg_position, '')
            """
        )
        
        # 3. REPLACE INTO из временной таблицы в целевую, фильтруя только валидные seller_id
        cursor.execute(f"""
            REPLACE INTO WB_ad_stats
            SELECT t.*
            FROM {temp_table} t
            INNER JOIN wb_sellers s ON s.seller_id = t.seller_id
        """)
        
    except Error as exc:
        raise RuntimeError(f"LOAD DATA INFILE for ad stats failed: {exc}") from exc


def load_ad_expenses_into_db(cursor: mysql.connector.cursor.MySQLCursor) -> None:
    try:
        cursor.execute(
            f"""
            LOAD DATA INFILE '{WB_AD_EXPENSES_CSV_MYSQL_PATH}'
            REPLACE
            INTO TABLE WB_ad_expenses
            CHARACTER SET utf8mb4
            FIELDS TERMINATED BY ','
            ENCLOSED BY '"'
            LINES TERMINATED BY '\\n'
            IGNORE 1 LINES
            (
              seller_advert_date_key,
              @sellerId,
              @advertId,
              @campName,
              @advertType,
              @paymentType,
              @advertStatus,
              @updNum,
              @updTime,
              @updSum
            )
            SET
              seller_id = NULLIF(@sellerId, ''),
              advertId = NULLIF(@advertId, ''),
              campName = NULLIF(@campName, ''),
              advertType = NULLIF(@advertType, ''),
              paymentType = NULLIF(@paymentType, ''),
              advertStatus = NULLIF(@advertStatus, ''),
              updNum = NULLIF(@updNum, ''),
              updTime = NULLIF(@updTime, ''),
              updSum = NULLIF(@updSum, '')
            """
        )
    except Error as exc:
        raise RuntimeError(f"LOAD DATA INFILE for ad expenses failed: {exc}") from exc


def load_stocks_into_db(cursor: mysql.connector.cursor.MySQLCursor) -> None:
    try:
        cursor.execute(
            f"""
            LOAD DATA INFILE '{WB_STOCKS_CSV_MYSQL_PATH}'
            REPLACE
            INTO TABLE WB_stocks
            CHARACTER SET utf8mb4
            FIELDS TERMINATED BY ','
            ENCLOSED BY '"'
            LINES TERMINATED BY '\\n'
            IGNORE 1 LINES
            (
              seller_bc_key,
              @sellerId,
              @barcode,
              @subjectName,
              @vendorCode,
              @nmId,
              @techSize,
              @volume,
              @quantity,
              @inWayToClient,
              @inWayFromClient,
              @quantityFull,
              @warehouses
            )
            SET
              seller_id = NULLIF(@sellerId, ''),
              barcode = NULLIF(@barcode, ''),
              subjectName = NULLIF(@subjectName, ''),
              vendorCode = NULLIF(@vendorCode, ''),
              nmId = NULLIF(@nmId, ''),
              techSize = NULLIF(@techSize, ''),
              volume = NULLIF(@volume, ''),
              quantity = NULLIF(@quantity, ''),
              inWayToClient = NULLIF(@inWayToClient, ''),
              inWayFromClient = NULLIF(@inWayFromClient, ''),
              quantityFull = NULLIF(@quantityFull, ''),
              warehouses = CASE
                WHEN @warehouses = '' OR @warehouses IS NULL THEN JSON_ARRAY()
                -- Прямая проверка валидности JSON (если уже правильно сериализован через json.dumps)
                WHEN JSON_VALID(@warehouses) THEN CAST(@warehouses AS JSON)
                -- Обработка экранированных кавычек из CSV: CSV writer удваивает кавычки внутри полей
                -- CSV writer экранирует кавычки, удваивая их, поэтому нужно заменить "" на "
                -- Это аналогично тому, как JSON.stringify работает в n8n
                WHEN JSON_VALID(REPLACE(@warehouses, '""', '"')) THEN CAST(REPLACE(@warehouses, '""', '"') AS JSON)
                -- Множественная замена двойных кавычек (на случай двойного/тройного экранирования)
                WHEN JSON_VALID(REPLACE(REPLACE(REPLACE(@warehouses, '""', '"'), '""', '"'), '""', '"')) 
                     THEN CAST(REPLACE(REPLACE(REPLACE(@warehouses, '""', '"'), '""', '"'), '""', '"') AS JSON)
                -- Новая схема: warehouses сохраняем в CSV в base64, чтобы избежать проблем с кавычками и разделителями
                WHEN JSON_VALID(CONVERT(FROM_BASE64(@warehouses) USING utf8mb4)) THEN CAST(CONVERT(FROM_BASE64(@warehouses) USING utf8mb4) AS JSON)
                ELSE JSON_ARRAY()
              END
            """
        )
    except Error as exc:
        raise RuntimeError(f"LOAD DATA INFILE for stocks failed: {exc}") from exc


def update_products_status(
    cursor: mysql.connector.cursor.MySQLCursor,
    sellers_meta: Iterable[Dict],
    products_rows: Iterable[Dict],
    api_error_sellers: Set[str],
) -> Tuple[int, List[Dict]]:
    per_seller: Dict[str, Dict] = {}

    for meta in sellers_meta:
        seller_id = str(meta.get("seller_id") or "").strip()
        if not seller_id:
            continue
        per_seller[seller_id] = {
            "prev": meta.get("products_status"),
            "brand": meta.get("brand") or meta.get("wb_api_brand") or seller_id,
            "api_error": seller_id in api_error_sellers,
            "cards": [],
        }

    for row in products_rows:
        seller_id = str(row.get("seller_id") or "").strip()
        if not seller_id:
            continue
        bucket = per_seller.setdefault(
            seller_id,
            {
                "prev": row.get("products_status"),
                "brand": row.get("brand") or seller_id,
                "api_error": seller_id in api_error_sellers,
                "cards": [],
            },
        )
        cards = row.get("cards") or []
        bucket["cards"].extend(cards)

    updates: List[Tuple[str, str]] = []
    details: List[Dict] = []
    now_time = msk_now().strftime("%Y-%m-%d %H:%M:%S")

    for seller_id, bucket in per_seller.items():
        prev = bucket.get("prev")

        if bucket.get("api_error"):
            if prev:
                products_status = prev
            else:
                continue
        else:
            cards = bucket.get("cards") or []

            nm_ids: Set[str] = set()
            imt_ids: Set[str] = set()
            subject_names: Set[str] = set()
            brand_names: Set[str] = set()
            videos: Set[str] = set()
            skus: Set[str] = set()
            dimensions_valid_count = 0
            need_kiz_count = 0

            for card in cards:
                nm_id = card.get("nmID")
                if nm_id:
                    nm_ids.add(str(nm_id))

                imt_id = card.get("imtID")
                if imt_id:
                    imt_ids.add(str(imt_id))

                subject_name = (card.get("subjectName") or "").strip()
                if subject_name:
                    subject_names.add(subject_name)

                brand_name = (card.get("brand") or "").strip()
                if brand_name:
                    brand_names.add(brand_name)

                video_url = (card.get("video") or "").strip()
                if video_url:
                    videos.add(video_url)

                dimensions = card.get("dimensions") or {}
                if dimensions.get("isValid"):
                    dimensions_valid_count += 1

                if card.get("needKiz"):
                    need_kiz_count += 1

                for size_variant in card.get("sizes") or []:
                    for sku in size_variant.get("skus") or []:
                        sku_val = str(sku).strip()
                        if sku_val:
                            skus.add(sku_val)

            products_status = {
                "nowTime": now_time,
                "nmID": len(nm_ids),
                "imtID": len(imt_ids),
                "subjectName": len(subject_names),
                "brand": len(brand_names),
                "video": len(videos),
                "dimensions_isValid": dimensions_valid_count,
                "skus": len(skus),
                "needKiz": need_kiz_count,
            }

        updates.append((json.dumps(products_status, ensure_ascii=False), seller_id))
        details.append(
            {
                "seller_id": seller_id,
                "brand": bucket.get("brand") or seller_id,
                "status": products_status,
            }
        )

    if updates:
        cursor.executemany(
            """
            UPDATE WB_sellers_updates
            SET products_status = %s
            WHERE seller_id = %s
            """,
            updates,
        )

    return len(updates), details


def update_ad_list_status(
    cursor: mysql.connector.cursor.MySQLCursor,
    sellers_meta: Iterable[Dict],
    ad_results: Iterable[Dict],
    api_error_sellers: Set[str],
) -> Tuple[int, List[Dict]]:
    per_seller: Dict[str, Dict] = {}

    for meta in sellers_meta:
        seller_id = str(meta.get("seller_id") or "").strip()
        if not seller_id:
            continue
        per_seller[seller_id] = {
            "prev": meta.get("ad_list_status"),
            "brand": meta.get("brand") or meta.get("wb_api_brand") or seller_id,
            "api_error": seller_id in api_error_sellers,
            "counts": None,
        }

    for result in ad_results:
        seller_id = str(result.get("seller_id") or "").strip()
        if not seller_id:
            continue
        bucket = per_seller.setdefault(
            seller_id,
            {
                "prev": result.get("ad_list_status"),
                "brand": result.get("brand") or seller_id,
                "api_error": seller_id in api_error_sellers,
                "counts": None,
            },
        )
        bucket["counts"] = result.get("counts")

    updates: List[Tuple[str, str]] = []
    details: List[Dict] = []
    now_time = msk_now().strftime("%Y-%m-%d %H:%M:%S")

    for seller_id, bucket in per_seller.items():
        prev = bucket.get("prev")

        if bucket.get("api_error"):
            if prev:
                ad_status = prev
            else:
                continue
        else:
            counts = bucket.get("counts") or {}
            all_cnt = int(counts.get("all") or 0)
            active_cnt = int(counts.get("active") or 0)
            paused_cnt = int(counts.get("paused") or 0)

            ad_status = {
                "nowTime": now_time,
                "ad_all_cnt": all_cnt,
                "ad_active_cnt": active_cnt,
                "ad_paused_cnt": paused_cnt,
            }

        updates.append((json.dumps(ad_status, ensure_ascii=False), seller_id))
        details.append(
            {
                "seller_id": seller_id,
                "brand": bucket.get("brand") or seller_id,
                "status": ad_status,
            }
        )

    if updates:
        cursor.executemany(
            """
            UPDATE WB_sellers_updates
            SET ad_list_status = %s
            WHERE seller_id = %s
            """,
            updates,
        )

    return len(updates), details


def update_stocks_status(
    cursor: mysql.connector.cursor.MySQLCursor,
    sellers_meta: Iterable[Dict],
    stocks_rows: Iterable[Dict],
    api_error_sellers: Set[str],
    successful_sellers: Set[str],
) -> Tuple[int, List[Dict]]:
    per_seller: Dict[str, Dict] = {}

    def safe_int(value: Any) -> int:
        if value is None:
            return 0
        if isinstance(value, (int, float)):
            return int(value)
        try:
            value_str = str(value).strip()
            if not value_str:
                return 0
            return int(float(value_str))
        except Exception:
            return 0

    for meta in sellers_meta:
        seller_id = str(meta.get("seller_id") or "").strip()
        if not seller_id:
            continue
        per_seller[seller_id] = {
            "prev": meta.get("stock_status"),
            "brand": meta.get("brand") or meta.get("wb_api_brand") or seller_id,
            "api_error": seller_id in api_error_sellers,
            "aggregates": None,
        }

    for row in stocks_rows:
        seller_id = str(row.get("seller_id") or "").strip()
        if not seller_id:
            continue
        bucket = per_seller.setdefault(
            seller_id,
            {
                "prev": row.get("stock_status"),
                "brand": row.get("brand") or seller_id,
                "api_error": seller_id in api_error_sellers,
                "aggregates": None,
            },
        )

        aggregates = bucket.get("aggregates")
        if aggregates is None:
            aggregates = {
                "all_stock_cnt": 0,
                "to_client_cnt": 0,
                "from_client_cnt": 0,
                "in_whsWB_cnt": 0,
                "nm_ids_positive": set(),
            }
            bucket["aggregates"] = aggregates

        all_stock_cnt = safe_int(row.get("quantityFull"))
        to_client_cnt = safe_int(row.get("inWayToClient"))
        from_client_cnt = safe_int(row.get("inWayFromClient"))
        in_whs_cnt = safe_int(row.get("quantity"))

        aggregates["all_stock_cnt"] += all_stock_cnt
        aggregates["to_client_cnt"] += to_client_cnt
        aggregates["from_client_cnt"] += from_client_cnt
        aggregates["in_whsWB_cnt"] += in_whs_cnt

        if in_whs_cnt > 0:
            nm_id = row.get("nmId")
            if nm_id is not None:
                aggregates["nm_ids_positive"].add(str(nm_id))

    updates: List[Tuple[str, str]] = []
    details: List[Dict] = []
    now_time = msk_now().strftime("%Y-%m-%d %H:%M:%S")

    for seller_id, bucket in per_seller.items():
        prev = bucket.get("prev")
        aggregates = bucket.get("aggregates")
        processed_successfully = aggregates is not None or seller_id in successful_sellers

        if not processed_successfully and not bucket.get("api_error"):
            continue

        if bucket.get("api_error"):
            if prev:
                stock_status = prev
            else:
                continue
        else:
            if not aggregates:
                stock_status = {
                    "nowTime": now_time,
                    "all_stock_cnt": 0,
                    "to_client_cnt": 0,
                    "from_client_cnt": 0,
                    "in_whsWB_cnt": 0,
                    "nmid_stock_cnt": 0,
                }
            else:
                stock_status = {
                    "nowTime": now_time,
                    "all_stock_cnt": int(aggregates.get("all_stock_cnt") or 0),
                    "to_client_cnt": int(aggregates.get("to_client_cnt") or 0),
                    "from_client_cnt": int(aggregates.get("from_client_cnt") or 0),
                    "in_whsWB_cnt": int(aggregates.get("in_whsWB_cnt") or 0),
                    "nmid_stock_cnt": len(aggregates.get("nm_ids_positive") or []),
                }

        updates.append((json.dumps(stock_status, ensure_ascii=False), seller_id))
        details.append(
            {
                "seller_id": seller_id,
                "brand": bucket.get("brand") or seller_id,
                "status": stock_status,
            }
        )

    if updates:
        cursor.executemany(
            """
            UPDATE WB_sellers_updates
            SET stock_status = %s
            WHERE seller_id = %s
            """,
            updates,
        )

    return len(updates), details


def update_ad_stats_status(
    cursor: mysql.connector.cursor.MySQLCursor,
    sellers_meta: Iterable[Dict],
    ad_stats_rows: Iterable[Dict],
    ad_stats_requests_by_seller: Dict[str, List[Dict]],
    ad_stats_successful_requests_by_seller: Dict[str, List[Dict]],
    ad_stats_failed_requests_by_seller: Dict[str, List[Dict]],
) -> Tuple[int, List[Dict]]:
    threshold180 = ymd_minus_days(AD_STATS_LOOKBACK_DAYS)

    def min_ymd(a: Optional[str], b: Optional[str]) -> Optional[str]:
        if a is None:
            return b
        if b is None:
            return a
        return a if a < b else b

    def max_ymd(a: Optional[str], b: Optional[str]) -> Optional[str]:
        if a is None:
            return b
        if b is None:
            return a
        return a if a > b else b

    per_seller: Dict[str, Dict] = {}

    for meta in sellers_meta:
        seller_id = str(meta.get("seller_id") or "").strip()
        if not seller_id:
            continue
        per_seller[seller_id] = {
            "rows": [],
            "prev": meta.get("ad_stats_status"),
            "brand": meta.get("brand") or meta.get("wb_api_brand") or seller_id,
            "campaign_ids": meta.get("campaign_ids_list", meta.get("campaign_ids", [])),
        }

    for row in ad_stats_rows:
        seller_id = str(row.get("seller_id") or "").strip()
        if not seller_id:
            continue
        bucket = per_seller.setdefault(
            seller_id,
            {
                "rows": [],
                "prev": row.get("ad_stats_status"),
                "brand": row.get("brand") or seller_id,
            },
        )
        bucket["rows"].append(row)
        if not bucket["prev"] and row.get("ad_stats_status"):
            bucket["prev"] = row["ad_stats_status"]

    updates: List[Tuple[str, str]] = []
    details: List[Dict] = []
    now_time = msk_now().strftime("%Y-%m-%d %H:%M:%S")

    for seller_id, bucket in per_seller.items():
        rows = bucket["rows"]
        count = len(rows)
        prev = bucket.get("prev")
        prev_status = (prev or {}).get("status") if prev else None
        prev_max_begin_date = first_ymd((prev or {}).get("maxBeginDate"))

        all_requests = ad_stats_requests_by_seller.get(seller_id, [])
        successful_requests = ad_stats_successful_requests_by_seller.get(seller_id, [])
        failed_requests = ad_stats_failed_requests_by_seller.get(seller_id, [])

        # Если все запросы провалились - не меняем статус
        if len(all_requests) > 0 and len(failed_requests) == len(all_requests):
            if prev:
                ad_stats_status = prev
            else:
                continue
        elif len(all_requests) == 0:
            # Нет запросов для селлера - проверяем, есть ли у него кампании
            campaign_ids = bucket.get("campaign_ids", [])
            if len(campaign_ids) == 0:
                # У селлера нет рекламных кампаний - ставим статус right
                ad_stats_status = {
                    "status": "right",
                    "nowTime": now_time,
                    "lastTotalRow": 0,
                    "leftBoundary": "",
                    "maxBeginDate": "",
                    "lastBeginDate": "",
                    "rightBoundary": "",
                }
            else:
                # Нет запросов по другой причине (не был в allowed_effective) - пропускаем
                continue
        else:
            # Вычисляем beginDate из успешных запросов
            current_begin_date = None
            for req in successful_requests:
                begin_date = first_ymd(req.get("interval", {}).get("beginDate"))
                if begin_date:
                    current_begin_date = min_ymd(current_begin_date, begin_date)

            # Границы по полю date из строк статистики
            left_boundary = None
            right_boundary = None
            for r in rows:
                date_str = first_ymd(r.get("date"))
                if date_str:
                    left_boundary = min_ymd(left_boundary, date_str)
                    right_boundary = max_ymd(right_boundary, date_str)

            # Если все запросы успешны но пустые (нет строк) - статус right (новичок)
            if count == 0 and len(successful_requests) > 0:
                new_max_begin_date = prev_max_begin_date or current_begin_date
                new_status = "right"
            elif not prev:
                # Впервые видим селлера
                new_max_begin_date = current_begin_date
                new_status = "left"
            else:
                # maxBeginDate = min(старый maxBeginDate, текущий beginDate)
                candidates = [prev_max_begin_date, current_begin_date]
                candidates = [c for c in candidates if c]
                new_max_begin_date = min(candidates) if candidates else (prev_max_begin_date or current_begin_date)

                # 'right' если старый уже 'right' ИЛИ текущий beginDate <= (МСК сегодня - 180 дней)
                cond_a = prev_status == "right"
                cond_b = current_begin_date and current_begin_date <= threshold180
                new_status = "right" if (cond_a or cond_b) else "left"

            ad_stats_status = {
                "status": new_status,
                "nowTime": now_time,
                "lastTotalRow": count,
                "leftBoundary": left_boundary or "",
                "maxBeginDate": new_max_begin_date or "",
                "lastBeginDate": current_begin_date or "",
                "rightBoundary": right_boundary or "",
            }

        updates.append((json.dumps(ad_stats_status, ensure_ascii=False), seller_id))
        details.append(
            {
                "seller_id": seller_id,
                "brand": bucket.get("brand") or seller_id,
                "from": prev_status if prev else "null",
                "to": ad_stats_status.get("status") if isinstance(ad_stats_status, dict) else (prev_status if prev else "null"),
                "count": count,
                "maxBeginDate": ad_stats_status.get("maxBeginDate") if isinstance(ad_stats_status, dict) else "",
            }
        )

    if updates:
        cursor.executemany(
            """
            UPDATE WB_sellers_updates
            SET ad_stats_status = %s
            WHERE seller_id = %s
            """,
            updates,
        )

    return len(updates), details


def update_ad_expenses_status(
    cursor: mysql.connector.cursor.MySQLCursor,
    sellers_meta: Iterable[Dict],
    ad_expenses_rows: Iterable[Dict],
    ad_expenses_requests_by_seller: Dict[str, List[Dict]],
    ad_expenses_successful_requests_by_seller: Dict[str, List[Dict]],
    ad_expenses_failed_requests_by_seller: Dict[str, List[Dict]],
) -> Tuple[int, List[Dict]]:
    
    def min_ymd(a: Optional[str], b: Optional[str]) -> Optional[str]:
        if a is None:
            return b
        if b is None:
            return a
        return a if a < b else b

    def max_ymd(a: Optional[str], b: Optional[str]) -> Optional[str]:
        if a is None:
            return b
        if b is None:
            return a
        return a if a > b else b

    per_seller: Dict[str, Dict] = {}

    for meta in sellers_meta:
        seller_id = str(meta.get("seller_id") or "").strip()
        if not seller_id:
            continue
        per_seller[seller_id] = {
            "rows": [],
            "prev": meta.get("ad_expenses_status"),
            "brand": meta.get("brand") or meta.get("wb_api_brand") or seller_id,
        }

    for row in ad_expenses_rows:
        seller_id = str(row.get("seller_id") or "").strip()
        if not seller_id:
            continue
        bucket = per_seller.setdefault(
            seller_id,
            {
                "rows": [],
                "prev": row.get("ad_expenses_status"),
                "brand": row.get("brand") or seller_id,
            },
        )
        bucket["rows"].append(row)
        if not bucket["prev"] and row.get("ad_expenses_status"):
            bucket["prev"] = row["ad_expenses_status"]

    updates: List[Tuple[str, str]] = []
    details: List[Dict] = []
    now_time = msk_now().strftime("%Y-%m-%d %H:%M:%S")

    for seller_id, bucket in per_seller.items():
        rows = bucket["rows"]
        count = len(rows)
        prev = bucket.get("prev")
        prev_status = (prev or {}).get("status") if prev else None
        prev_max_begin_date = first_ymd((prev or {}).get("maxBeginDate"))

        all_requests = ad_expenses_requests_by_seller.get(seller_id, [])
        successful_requests = ad_expenses_successful_requests_by_seller.get(seller_id, [])
        failed_requests = ad_expenses_failed_requests_by_seller.get(seller_id, [])

        # Если все запросы провалились - не меняем статус
        if len(all_requests) > 0 and len(failed_requests) == len(all_requests):
            if prev:
                ad_expenses_status = prev
            else:
                continue
        elif len(all_requests) == 0:
            # Нет запросов для селлера - проверяем, есть ли у него установленный статус "right" из-за 0 РК
            # (такой статус мог быть создан в prepare_ad_expenses_requests или fetch_ad_expenses_sellers)
            if prev and prev.get("status") == "right" and prev.get("lastTotalRow") == 0:
                # Сохраняем существующий статус "right" для селлера с 0 РК
                # Обновляем только nowTime, чтобы зафиксировать время последней проверки
                ad_expenses_status = {
                    "status": "right",
                    "nowTime": now_time,
                    "lastTotalRow": 0,
                    "leftBoundary": "",
                    "maxBeginDate": "",
                    "lastBeginDate": "",
                    "rightBoundary": "",
                }
            else:
                # Нет запросов по другой причине - пропускаем
                continue
        else:
            # Вычисляем beginDate из успешных запросов
            current_begin_date = None
            for req in successful_requests:
                begin_date = first_ymd(req.get("beginDate"))
                if begin_date:
                    current_begin_date = min_ymd(current_begin_date, begin_date)

            # Границы по полю updTime (дата) из строк списаний
            left_boundary = None
            right_boundary = None
            for r in rows:
                # updTime может быть датой в формате YYYY-MM-DD или пустой строкой
                date_str = first_ymd(r.get("updTime"))
                if date_str:
                    left_boundary = min_ymd(left_boundary, date_str)
                    right_boundary = max_ymd(right_boundary, date_str)

            if not prev:
                # Впервые видим селлера
                new_max_begin_date = current_begin_date
                # Если запросы вернули пустой ответ - это не значит, что все данные собраны
                # Продолжаем с left статусом
                new_status = "left"
            else:
                # maxBeginDate = min(старый maxBeginDate, текущий beginDate)
                candidates = [prev_max_begin_date, current_begin_date]
                candidates = [c for c in candidates if c]
                new_max_begin_date = min(candidates) if candidates else (prev_max_begin_date or current_begin_date)

                # 'right' если старый уже 'right' ИЛИ текущий beginDate <= (МСК сегодня - 180 дней)
                # НЕ ставим right только из-за пустого ответа - это может быть просто отсутствие списаний за период
                threshold180 = ymd_minus_days(AD_EXPENSES_LOOKBACK_DAYS)
                cond_a = prev_status == "right"
                cond_b = current_begin_date and current_begin_date <= threshold180
                new_status = "right" if (cond_a or cond_b) else "left"

            ad_expenses_status = {
                "status": new_status,
                "nowTime": now_time,
                "lastTotalRow": count,
                "leftBoundary": left_boundary or "",
                "maxBeginDate": new_max_begin_date or "",
                "lastBeginDate": current_begin_date or "",
                "rightBoundary": right_boundary or "",
            }

        updates.append((json.dumps(ad_expenses_status, ensure_ascii=False), seller_id))
        details.append(
            {
                "seller_id": seller_id,
                "brand": bucket.get("brand") or seller_id,
                "from": prev_status if prev else "null",
                "to": ad_expenses_status.get("status") if isinstance(ad_expenses_status, dict) else (prev_status if prev else "null"),
                "count": count,
                "maxBeginDate": ad_expenses_status.get("maxBeginDate") if isinstance(ad_expenses_status, dict) else "",
            }
        )

    if updates:
        cursor.executemany(
            """
            UPDATE WB_sellers_updates
            SET ad_expenses_status = %s
            WHERE seller_id = %s
            """,
            updates,
        )

    return len(updates), details


def update_sales_status(
    cursor: mysql.connector.cursor.MySQLCursor,
    sellers_meta: Iterable[Dict],
    sales_rows: Iterable[Dict],
    api_error_sellers: Set[str],
) -> int:
    threshold180 = ymd_minus_days(180)

    def min_ymd(a: Optional[str], b: Optional[str]) -> Optional[str]:
        if a is None:
            return b
        if b is None:
            return a
        return a if a < b else b

    def max_ymd(a: Optional[str], b: Optional[str]) -> Optional[str]:
        if a is None:
            return b
        if b is None:
            return a
        return a if a > b else b

    per_seller: Dict[str, Dict] = {}

    for meta in sellers_meta:
        seller_id = str(meta.get("seller_id") or "").strip()
        if not seller_id:
            continue
        per_seller[seller_id] = {
            "rows": [],
            "prev": meta.get("sales_status"),
            "date_from": first_ymd(meta.get("date_from") or meta.get("dateFrom")),
            "api_error": seller_id in api_error_sellers,
            "cooldown_blocked": False,
            "brand": meta.get("brand") or meta.get("wb_api_brand") or seller_id,
        }

    for row in sales_rows:
        seller_id = str(row.get("seller_id") or "").strip()
        if not seller_id:
            continue

        bucket = per_seller.setdefault(
            seller_id,
            {
                "rows": [],
                "prev": row.get("sales_status"),
                "date_from": first_ymd(row.get("dateFrom")),
                "api_error": seller_id in api_error_sellers,
                "cooldown_blocked": False,
                "brand": row.get("wb_api_brand") or seller_id,
            },
        )
        bucket["rows"].append(row)
        if not bucket["prev"] and row.get("sales_status"):
            bucket["prev"] = row["sales_status"]
        if not bucket["date_from"] and row.get("dateFrom"):
            bucket["date_from"] = first_ymd(row.get("dateFrom"))

    updates: List[Tuple[str, str]] = []
    details: List[Dict] = []
    now_time = msk_now().strftime("%Y-%m-%d %H:%M:%S")

    for seller_id, bucket in per_seller.items():
        rows = bucket["rows"]
        count = len(rows)
        prev = bucket["prev"]
        current_date_from = first_ymd(bucket.get("date_from"))
        if not current_date_from:
            current_date_from = first_ymd((prev or {}).get("lastDateFrom")) or first_ymd((prev or {}).get("maxDateFrom"))
        prev_max_date_from = first_ymd((prev or {}).get("maxDateFrom"))

        if bucket.get("api_error"):
            if prev:
                sales_status = prev
            else:
                continue
        elif count == 0:
            max_date_from = prev_max_date_from or current_date_from
            sales_status = {
                "maxDateFrom": max_date_from,
                "status": "right",
                "lastTotalSales": 0,
                "leftBoundary": "",
                "rightBoundary": "",
                "lastDateFrom": current_date_from,
                "nowTime": now_time,
            }
            if max_date_from is None:
                sales_status["maxDateFrom"] = ""
            if sales_status["lastDateFrom"] is None:
                sales_status["lastDateFrom"] = ""
        else:
            left_boundary = None
            right_boundary = None
            for r in rows:
                ymd_val = first_ymd(r.get("date")) or first_ymd(r.get("lastChangeDate"))
                if ymd_val:
                    left_boundary = min_ymd(left_boundary, ymd_val)
                    right_boundary = max_ymd(right_boundary, ymd_val)

            prev_status = (prev or {}).get("status")

            if not prev:
                new_max_date_from = current_date_from or None
                new_status = "left"
            else:
                candidates = [
                    first_ymd(current_date_from),
                    prev_max_date_from,
                ]
                candidates = [c for c in candidates if c]
                new_max_date_from = min(candidates) if candidates else None

                count_ok = 0 < count < 80000
                cond_b = current_date_from and current_date_from <= threshold180 and count_ok
                cond_c = prev_max_date_from and prev_max_date_from <= threshold180 and count_ok

                if prev_status == "right" or cond_b or cond_c:
                    new_status = "right"
                else:
                    new_status = "left"

            sales_status = {
                "maxDateFrom": new_max_date_from,
                "status": new_status,
                "lastTotalSales": count,
                "leftBoundary": left_boundary,
                "rightBoundary": right_boundary,
                "lastDateFrom": current_date_from,
                "nowTime": now_time,
            }

        if sales_status:
            updates.append((json.dumps(sales_status, ensure_ascii=False), seller_id))
            details.append(
                {
                    "seller_id": seller_id,
                    "brand": bucket.get("brand") or seller_id,
                    "from": (prev or {}).get("status") if prev else "null",
                    "to": sales_status.get("status") if isinstance(sales_status, dict) else (prev or {}).get("status"),
                    "count": count,
                    "date_from": current_date_from,
                }
            )

    if updates:
        cursor.executemany(
            """
            UPDATE WB_sellers_updates
            SET sales_status = %s
            WHERE seller_id = %s
            """,
            updates,
        )

    return len(updates), details


def main() -> int:
    connection = None
    cursor = None
    cursor_dict = None
    try:
        workflow_started_at = time.time()
        connection = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = connection.cursor()
        cursor_dict = connection.cursor(dictionary=True)

        sync_active_list(cursor)
        connection.commit()

        access_rows = fetch_access_list(cursor_dict)
        start_message = build_start_message(access_rows)
        print(
            f"[WB-bot] В работу взяты {len(access_rows)} брендов. Сообщение отправлено в Telegram.",
            flush=True,
        )
        send_to_telegram(start_message)

        orders_started_at = time.time()

        sellers = fetch_active_sellers(cursor_dict)
        allowed_items, aggregated, summary_text = compute_date_from_and_priority(sellers)
        print(
            "[WB-bot] Рассчитаны dateFrom и приоритеты для селлеров.",
            flush=True,
        )
        for item in allowed_items:
            print(
                f"[WB-bot] → {item['seller_id']} | {item['brand']} | dateFrom={item['date_from']}",
                flush=True,
            )

        summary_message = build_summary_message(summary_text)
        send_to_telegram(summary_message)

        cooldown_orders = [
            s for s in aggregated
            if s["status_cat"] == "right" and not s["allowed"] and s.get("cooldown_blocked")
        ]
        for s in cooldown_orders:
            remaining = right_status_cooldown_minutes(s.get("orders_status"))
            print(
                f"[WB-bot] Отложили селлера {s['seller_id']} ({s.get('brand')}) — повторная выгрузка через ~{remaining:.1f} мин.",
                flush=True,
            )

        all_orders: List[Dict] = []
        orders_api_errors: Set[str] = set()
        for item in allowed_items:
            token = (item.get("wb_api_key") or "").strip()
            if not token:
                print(
                    f"[WB-bot] Пропускаем {item['seller_id']} — отсутствует wb_api_key.",
                    flush=True,
                )
                orders_api_errors.add(item["seller_id"])
                continue
            try:
                orders = fetch_orders_for_seller(item)
                count = len(orders)
                print(
                    f"[WB-bot] Получено {count} заказов для {item['seller_id']} "
                    f"({item['brand']}).",
                    flush=True,
                )
                all_orders.extend(orders)
            except Exception as api_err:
                print(
                    f"[WB-bot] Ошибка WB API для {item['seller_id']}: {api_err}",
                    file=sys.stderr,
                    flush=True,
                )
                orders_api_errors.add(item["seller_id"])
                error_message = (
                    "<b>WB_orders</b>\n"
                    "<blockquote>"
                    f"Ошибка запроса для селлера <code>{html.escape(item['brand'])}</code>.\n"
                    f"<code>{html.escape(str(api_err))}</code>"
                    "</blockquote>"
                )
                try:
                    send_to_telegram(error_message)
                except Exception:
                    traceback.print_exc()

        if all_orders:
            orders_summary = build_orders_summary(all_orders)
            print("[WB-bot] Итоги по заказам:\n" + orders_summary, flush=True)
            summary_orders_message = (
                "<b>01 WB API</b> | Orders\n"
                "<blockquote><b>Собраны данные из WB</b>\n"
                f"<code>{html.escape(orders_summary)}</code></blockquote>"
            )
            send_to_telegram(summary_orders_message)

            csv_rows = convert_orders_for_csv(all_orders)
            write_orders_csv(csv_rows, WB_CSV_HOST_PATH)
            print(
                f"[WB-bot] CSV сформирован: {WB_CSV_HOST_PATH} ({len(csv_rows)} строк).",
                flush=True,
            )
            summary_csv_message = (
                "<b>01 WB API</b> | Orders\n"
                "<blockquote>Файл WB_orders_import.csv сформирован.</blockquote>"
            )
            send_to_telegram(summary_csv_message)

            load_orders_into_db(cursor)
            print("[WB-bot] WB_orders успешно обновлена через LOAD DATA INFILE.", flush=True)
            summary_load_message = (
                "<b>01 WB API</b> | Orders\n"
                "<blockquote>Данные загружены в таблицу WB_orders.</blockquote>"
            )
            send_to_telegram(summary_load_message)
        else:
            print("[WB-bot] Нет заказов для отправки итогов.", flush=True)

        updated_count, order_details = update_orders_status(cursor, allowed_items, all_orders, orders_api_errors)
        if updated_count:
            connection.commit()
            print(
                f"[WB-bot] Обновлены orders_status для {updated_count} селлеров.",
                flush=True,
            )
            if order_details:
                status_message = build_status_update_message("01", "Orders", order_details)
                send_to_telegram(status_message)

        orders_elapsed = time.time() - orders_started_at
        orders_completion_message = build_completion_message("01", "Orders", orders_elapsed)
        send_to_telegram(orders_completion_message)
        time.sleep(2)

        sales_started_at = time.time()

        sales_sellers = fetch_sales_sellers(cursor_dict)
        sales_allowed_items, sales_aggregated, sales_selection_summary = compute_sales_date_from_and_priority(sales_sellers)
        print("[WB-bot] Подготовлена выборка для продаж.", flush=True)
        sales_selection_message = build_sales_selection_message(sales_selection_summary)
        send_to_telegram(sales_selection_message)

        cooldown_sales = [
            s for s in sales_aggregated
            if s["status_cat"] == "right" and not s["allowed"] and s.get("cooldown_blocked")
        ]
        for s in cooldown_sales:
            remaining = right_status_cooldown_minutes(s.get("sales_status"))
            print(
                f"[WB-bot] Отложили продажи селлера {s['seller_id']} ({s.get('brand')}) — повтор через ~{remaining:.1f} мин.",
                flush=True,
            )

        sales_data: List[Dict] = []
        sales_api_errors: Set[str] = set()
        for item in sales_allowed_items:
            token = (item.get("wb_api_key") or "").strip()
            if not token:
                print(
                    f"[WB-bot] Пропускаем {item['seller_id']} в продажах — отсутствует wb_api_key.",
                    flush=True,
                )
                sales_api_errors.add(item["seller_id"])
                continue
            try:
                rows = fetch_sales_for_seller(item)
                print(
                    f"[WB-bot] Продажи: получено {len(rows)} строк для {item['seller_id']} ({item['brand']}).",
                    flush=True,
                )
                sales_data.extend(rows)
            except Exception as api_err:
                print(
                    f"[WB-bot] Ошибка WB Sales API для {item['seller_id']}: {api_err}",
                    file=sys.stderr,
                    flush=True,
                )
                sales_api_errors.add(item["seller_id"])
                error_message = (
                    "<b>WB_sales</b>\n"
                    "<blockquote>"
                    f"Ошибка запроса для селлера <code>{html.escape(item['brand'])}</code>.\n"
                    f"<code>{html.escape(str(api_err))}</code>"
                    "</blockquote>"
                )
                try:
                    send_to_telegram(error_message)
                except Exception:
                    traceback.print_exc()

        if sales_data:
            sales_summary = build_sales_summary(sales_data)
            print("[WB-bot] Итоги по продажам:\n" + sales_summary, flush=True)
            summary_sales_message = (
                "<b>02 WB API</b> | Sales\n"
                "<blockquote><b>Собраны данные по продажам</b>\n"
                f"<code>{html.escape(sales_summary)}</code></blockquote>"
            )
            send_to_telegram(summary_sales_message)

            sales_csv_rows = convert_sales_for_csv(sales_data)
            write_sales_csv(sales_csv_rows, WB_SALES_CSV_HOST_PATH)
            print(
                f"[WB-bot] CSV продаж сформирован: {WB_SALES_CSV_HOST_PATH} ({len(sales_csv_rows)} строк).",
                flush=True,
            )
            summary_sales_csv_message = (
                "<b>02 WB API</b> | Sales\n"
                "<blockquote>Файл WB_sales_import.csv сформирован.</blockquote>"
            )
            send_to_telegram(summary_sales_csv_message)

            load_sales_into_db(cursor)
            print("[WB-bot] WB_sales успешно обновлена через LOAD DATA INFILE.", flush=True)
            summary_sales_load_message = (
                "<b>02 WB API</b> | Sales\n"
                "<blockquote>Данные загружены в таблицу WB_sales.</blockquote>"
            )
            send_to_telegram(summary_sales_load_message)

        else:
            print("[WB-bot] Нет данных по продажам или все запросы завершились ошибкой.", flush=True)

        updated_sales_count, sales_details = update_sales_status(cursor, sales_allowed_items, sales_data, sales_api_errors)
        if updated_sales_count:
            connection.commit()
            print(
                f"[WB-bot] Обновлены sales_status для {updated_sales_count} селлеров.",
                flush=True,
            )
            if sales_details:
                status_sales_message = build_status_update_message("02", "Sales", sales_details)
                send_to_telegram(status_sales_message)
        elif not sales_data and sales_api_errors:
            print("[WB-bot] Статусы продаж не обновлялись из-за ошибок API.", flush=True)

        sales_elapsed = time.time() - sales_started_at
        sales_completion_message = build_completion_message("02", "Sales", sales_elapsed)
        send_to_telegram(sales_completion_message)
        time.sleep(2)

        products_started_at = time.time()

        products_sellers = fetch_products_sellers(cursor_dict)
        products_allowed_items, products_aggregated, products_summary = compute_products_priority(products_sellers)
        print("[WB-bot] Подготовлена выборка для карточек товаров.", flush=True)
        products_selection_message = build_products_selection_message(products_summary)
        send_to_telegram(products_selection_message)

        products_data: List[Dict] = []
        products_api_errors: Set[str] = set()
        for item in products_allowed_items:
            token = (item.get("wb_api_key") or "").strip()
            if not token:
                print(
                    f"[WB-bot] Пропускаем {item['seller_id']} в карточках — отсутствует wb_api_key.",
                    flush=True,
                )
                products_api_errors.add(item["seller_id"])
                continue
            try:
                cards = fetch_products_for_seller(item)
                print(
                    f"[WB-bot] Карточки: получено {len(cards)} для {item['seller_id']} ({item['brand']}).",
                    flush=True,
                )
                products_data.append(
                    {
                        "seller_id": item["seller_id"],
                        "cards": cards,
                        "brand": item.get("brand"),
                        "products_status": item.get("products_status"),
                    }
                )
            except Exception as api_err:
                print(
                    f"[WB-bot] Ошибка WB Products API для {item['seller_id']}: {api_err}",
                    file=sys.stderr,
                    flush=True,
                )
                products_api_errors.add(item["seller_id"])
                error_message = (
                    "<b>WB_products</b>\n"
                    "<blockquote>"
                    f"Ошибка запроса для селлера <code>{html.escape(item['brand'])}</code>.\n"
                    f"<code>{html.escape(str(api_err))}</code>"
                    "</blockquote>"
                )
                try:
                    send_to_telegram(error_message)
                except Exception:
                    traceback.print_exc()

        products_csv_rows = convert_products_for_csv(products_data)
        if products_csv_rows:
            write_products_csv(products_csv_rows, WB_PRODUCTS_CSV_HOST_PATH)
            print(
                f"[WB-bot] CSV карточек сформирован: {WB_PRODUCTS_CSV_HOST_PATH} ({len(products_csv_rows)} строк).",
                flush=True,
            )
            products_csv_message = (
                "<b>03 WB API</b> | Products\n"
                "<blockquote>Файл WB_products_import.csv сформирован.</blockquote>"
            )
            send_to_telegram(products_csv_message)

            try:
                load_products_into_db(cursor)
                print("[WB-bot] WB_products успешно обновлена через LOAD DATA INFILE.", flush=True)
                products_load_message = (
                    "<b>03 WB API</b> | Products\n"
                    "<blockquote>Данные загружены в таблицу WB_products.</blockquote>"
                )
                send_to_telegram(products_load_message)
            except Exception as db_err:
                print(
                    f"[WB-bot] Ошибка LOAD DATA для карточек: {db_err}",
                    file=sys.stderr,
                    flush=True,
                )
                error_message = (
                    "<b>03 WB API</b> | Products\n"
                    "<blockquote>"
                    "Ошибка загрузки данных в WB_products.\n"
                    f"<code>{html.escape(str(db_err))}</code>"
                    "</blockquote>"
                )
                try:
                    send_to_telegram(error_message)
                except Exception:
                    traceback.print_exc()
        else:
            print("[WB-bot] Нет данных для формирования CSV карточек.", flush=True)

        updated_products_count, products_details = update_products_status(cursor, products_allowed_items, products_data, products_api_errors)
        if updated_products_count:
            connection.commit()
            print(
                f"[WB-bot] Обновлены products_status для {updated_products_count} селлеров.",
                flush=True,
            )
            if products_details:
                status_products_message = build_products_status_message(products_details)
                send_to_telegram(status_products_message)
        elif products_api_errors:
            print("[WB-bot] Статусы продуктов не обновлялись из-за ошибок API.", flush=True)

        products_elapsed = time.time() - products_started_at
        products_completion_message = build_completion_message("03", "Products", products_elapsed)
        send_to_telegram(products_completion_message)
        time.sleep(2)

        ads_started_at = time.time()

        ad_sellers = fetch_ad_list_sellers(cursor_dict)
        ad_allowed_items, ad_aggregated, ad_summary = compute_ad_list_priority(ad_sellers)
        print("[WB-bot] Подготовлена выборка для рекламных кампаний.", flush=True)
        ad_selection_message = build_ad_list_selection_message(ad_summary)
        send_to_telegram(ad_selection_message)

        ad_results: List[Dict] = []
        ad_rows: List[Dict] = []
        ad_api_errors: Set[str] = set()

        allowed_ad_ids = {item["seller_id"] for item in ad_allowed_items}
        for s in ad_aggregated:
            if s["seller_id"] in allowed_ad_ids or s["status_cat"] == "new":
                continue
            status_dict = s.get("ad_list_status") or {}
            now_dt = parse_msk_datetime(status_dict.get("nowTime") or s.get("now_time"))
            if now_dt is None:
                continue
            age_min = minutes_since_msk(now_dt)
            remaining = ADS_COOLDOWN_MINUTES - age_min
            if remaining > 0:
                print(
                    f"[WB-bot] Отложили рекламные кампании {s['seller_id']} ({s.get('brand')}) — повтор через ~{remaining:.1f} мин.",
                    flush=True,
                )

        for item in ad_allowed_items:
            token = (item.get("wb_api_key") or "").strip()
            if not token:
                print(
                    f"[WB-bot] Пропускаем {item['seller_id']} в рекламе — отсутствует wb_api_key.",
                    flush=True,
                )
                ad_api_errors.add(item["seller_id"])
                continue
            try:
                result = fetch_ad_list_for_seller(item)
                ad_results.append(result)
                rows = result.get("rows") or []
                counts = result.get("counts") or {}
                print(
                    "[WB-bot] Рекламные кампании: "
                    f"all={counts.get('all', 0)} active={counts.get('active', 0)} paused={counts.get('paused', 0)} "
                    f"для {item['seller_id']} ({item['brand']}).",
                    flush=True,
                )
                ad_rows.extend(rows)
            except Exception as api_err:
                print(
                    f"[WB-bot] Ошибка WB Ad List API для {item['seller_id']}: {api_err}",
                    file=sys.stderr,
                    flush=True,
                )
                ad_api_errors.add(item["seller_id"])
                error_message = (
                    "<b>WB_ad_list</b>\n"
                    "<blockquote>"
                    f"Ошибка запроса для селлера <code>{html.escape(item['brand'])}</code>.\n"
                    f"<code>{html.escape(str(api_err))}</code>"
                    "</blockquote>"
                )
                try:
                    send_to_telegram(error_message)
                except Exception:
                    traceback.print_exc()

        if ad_rows:
            write_ad_list_csv(ad_rows, WB_AD_LIST_CSV_HOST_PATH)
            print(
                f"[WB-bot] CSV реклам сформирован: {WB_AD_LIST_CSV_HOST_PATH} ({len(ad_rows)} строк).",
                flush=True,
            )
            ad_csv_message = (
                "<b>04 WB API</b> | Ad List\n"
                "<blockquote>Файл WB_ad_list_import.csv сформирован.</blockquote>"
            )
            send_to_telegram(ad_csv_message)
        else:
            print("[WB-bot] Нет данных по рекламным кампаниям.", flush=True)

        if ad_rows:
            try:
                load_ad_list_into_db(cursor)
                print("[WB-bot] WB_ad_campaigns успешно обновлена через LOAD DATA INFILE.", flush=True)
                ad_load_message = (
                    "<b>04 WB API</b> | Ad List\n"
                    "<blockquote>Данные загружены в таблицу WB_ad_campaigns.</blockquote>"
                )
                send_to_telegram(ad_load_message)
            except Exception as db_err:
                print(f"[WB-bot] Ошибка LOAD DATA для реклам: {db_err}", file=sys.stderr, flush=True)
                error_message = (
                    "<b>04 WB API</b> | Ad List\n"
                    "<blockquote>"
                    "Ошибка загрузки данных в WB_ad_campaigns.\n"
                    f"<code>{html.escape(str(db_err))}</code>"
                    "</blockquote>"
                )
                try:
                    send_to_telegram(error_message)
                except Exception:
                    traceback.print_exc()

        ad_status_updates, ad_status_details = update_ad_list_status(cursor, ad_allowed_items, ad_results, ad_api_errors)
        if ad_status_updates:
            connection.commit()
            print(
                f"[WB-bot] Обновлены ad_list_status для {ad_status_updates} селлеров.",
                flush=True,
            )
            if ad_status_details:
                ad_status_message = build_ad_list_status_message(ad_status_details)
                send_to_telegram(ad_status_message)
        elif ad_api_errors:
            print("[WB-bot] Статусы реклам не обновлялись из-за ошибок API.", flush=True)

        ads_elapsed = time.time() - ads_started_at
        ads_completion_message = build_completion_message("04", "Ad List", ads_elapsed)
        send_to_telegram(ads_completion_message)
        time.sleep(2)

        ad_stats_started_at = time.time()
        ad_stats_sellers = fetch_ad_stats_sellers(cursor_dict)
        ad_stats_requests, ad_stats_processed, ad_stats_summary = prepare_ad_stats_requests(ad_stats_sellers)
        print("[WB-bot] Подготовлена выборка для статистики рекламных кампаний.", flush=True)
        ad_stats_selection_message = build_ad_stats_selection_message(ad_stats_summary)
        send_to_telegram(ad_stats_selection_message)

        ad_stats_rows: List[Dict] = []
        ad_stats_rows_by_seller: Dict[str, int] = defaultdict(int)
        ad_stats_campaigns_with_data: Dict[str, Set[int]] = defaultdict(set)
        ad_stats_empty_campaigns: Dict[str, int] = defaultdict(int)
        ad_stats_errors: List[str] = []
        seller_brand_map: Dict[str, str] = {}
        ad_stats_requests_by_seller: Dict[str, List[Dict]] = defaultdict(list)
        ad_stats_successful_requests_by_seller: Dict[str, List[Dict]] = defaultdict(list)
        ad_stats_failed_requests_by_seller: Dict[str, List[Dict]] = defaultdict(list)

        if ad_stats_requests:
            unique_sellers = len({req["seller_id"] for req in ad_stats_requests})
            ad_stats_plan_message = build_ad_stats_plan_message(ad_stats_requests, unique_sellers)
            send_to_telegram(ad_stats_plan_message)
            print(
                f"[WB-bot] Запланировано {len(ad_stats_requests)} запросов статистики для {unique_sellers} селлеров.",
                flush=True,
            )

            sorted_ad_stats_requests = sorted(ad_stats_requests, key=lambda r: r.get("ready_at_ts", 0.0))
            ad_stats_last_request_ts: Dict[str, float] = {}
            total_requests = len(sorted_ad_stats_requests)
            for request_index, req in enumerate(sorted_ad_stats_requests, start=1):
                seller_id = req["seller_id"]
                brand = req["brand"]
                ad_stats_requests_by_seller[seller_id].append(req)
                seller_brand_map[seller_id] = brand

                begin_short = format_date_short(req["interval"]["beginDate"])
                end_short = format_date_short(req["interval"]["endDate"])
                print(
                    f"[WB-bot] Ad Stats: запрос {request_index}/{total_requests} | "
                    f"{brand} | chunk {req['chunk_index']}/{req['chunk_total']} | "
                    f"{begin_short}→{end_short}",
                    flush=True,
                )

                now_ts = time.time()
                wait_seconds = max(0.0, req.get("ready_at_ts", 0.0) - now_ts)
                last_request_ts = ad_stats_last_request_ts.get(seller_id)
                if last_request_ts is not None:
                    wait_seconds = max(wait_seconds, (last_request_ts + AD_STATS_RATE_INTERVAL_SECONDS) - now_ts)
                if wait_seconds > 0:
                    time.sleep(wait_seconds)

                params = {
                    "beginDate": req["interval"]["beginDate"],
                    "endDate": req["interval"]["endDate"],
                    "ids": req["campaign_ids_csv"],
                }
                headers = {
                    "user-agent": WB_USER_AGENT,
                    "Authorization": f"Bearer {req['token']}",
                }

                success = False

                for attempt in range(1, AD_STATS_MAX_RETRIES + 1):
                    try:
                        response = requests.get(WB_AD_STATS_URL, params=params, headers=headers, timeout=40)
                        ad_stats_last_request_ts[seller_id] = time.time()
                        status_code = response.status_code
                        response_text = (response.text or "").strip()

                        if status_code == 200:
                            payload = response.json()
                            parsed_rows = parse_ad_stats_payload(payload)

                            if parsed_rows:
                                ad_stats_rows_by_seller[seller_id] += len(parsed_rows)
                                ad_stats_campaigns_with_data[seller_id].update(
                                    {row.get("advertId") for row in parsed_rows if row.get("advertId") is not None}
                                )
                                for row in parsed_rows:
                                    row_out = dict(row)
                                    row_out["seller_id"] = seller_id
                                    advert_id = row.get("advertId")
                                    date_str = row.get("date")
                                    app_type = row.get("appType")
                                    nm_id = row.get("nmId")
                                    
                                    # Формируем seller_advert_date_key только если все обязательные поля присутствуют
                                    if seller_id and advert_id is not None and date_str and app_type is not None:
                                        row_out["seller_advert_date_key"] = f"{seller_id}_{advert_id}_{nm_id or ''}_{date_str}_{app_type}"
                                        ad_stats_rows.append(row_out)
                                    else:
                                        # Пропускаем строки с неполными данными, чтобы избежать ошибки дубликата пустого ключа
                                        missing_fields = []
                                        if not seller_id:
                                            missing_fields.append("seller_id")
                                        if advert_id is None:
                                            missing_fields.append("advertId")
                                        if not date_str:
                                            missing_fields.append("date")
                                        if app_type is None:
                                            missing_fields.append("appType")
                                        print(
                                            f"[WB-bot] Пропущена строка ad_stats с неполными данными (отсутствуют: {', '.join(missing_fields)})",
                                            flush=True,
                                        )
                            else:
                                ad_stats_empty_campaigns[seller_id] += len(req["campaign_ids"])

                            ad_stats_successful_requests_by_seller[seller_id].append(req)
                            success = True
                            break

                        if status_code == 400 and "no statistics" in response_text.lower():
                            ad_stats_empty_campaigns[seller_id] += len(req["campaign_ids"])
                            ad_stats_successful_requests_by_seller[seller_id].append(req)
                            success = True
                            break

                        if status_code in {429, 500}:
                            if attempt == AD_STATS_MAX_RETRIES:
                                ad_stats_failed_requests_by_seller[seller_id].append(req)
                                error_line = (
                                    f"{brand} | chunk {req['chunk_index']}/{req['chunk_total']} | "
                                    f"{req['interval']['beginDate']}→{req['interval']['endDate']} | "
                                    f"{status_code} {response_text}"
                                )
                                ad_stats_errors.append(error_line)
                                print(
                                    f"[WB-bot] Ошибка WB Ad Stats API: {error_line}",
                                    file=sys.stderr,
                                    flush=True,
                                )
                                break
                            delay = AD_STATS_RETRY_BASE_DELAY_SECONDS * attempt
                            time.sleep(delay)
                            continue

                        ad_stats_failed_requests_by_seller[seller_id].append(req)
                        error_line = (
                            f"{brand} | chunk {req['chunk_index']}/{req['chunk_total']} | "
                            f"{req['interval']['beginDate']}→{req['interval']['endDate']} | "
                            f"{status_code} {response_text}"
                        )
                        ad_stats_errors.append(error_line)
                        print(
                            f"[WB-bot] Ошибка WB Ad Stats API: {error_line}",
                            file=sys.stderr,
                            flush=True,
                        )
                        break
                    except Exception as api_err:
                        ad_stats_last_request_ts[seller_id] = time.time()
                        if attempt == AD_STATS_MAX_RETRIES:
                            ad_stats_failed_requests_by_seller[seller_id].append(req)
                            error_line = (
                                f"{brand} | chunk {req['chunk_index']}/{req['chunk_total']} | "
                                f"{req['interval']['beginDate']}→{req['interval']['endDate']} | {api_err}"
                            )
                            ad_stats_errors.append(error_line)
                            print(
                                f"[WB-bot] Ошибка WB Ad Stats API: {error_line}",
                                file=sys.stderr,
                                flush=True,
                            )
                            break
                        delay = AD_STATS_RETRY_BASE_DELAY_SECONDS * attempt
                        time.sleep(delay)

                if not success:
                    continue

            if ad_stats_rows:
                write_ad_stats_csv(ad_stats_rows, WB_AD_STATS_CSV_HOST_PATH)
                print(
                    f"[WB-bot] CSV статистики реклам сформирован: {WB_AD_STATS_CSV_HOST_PATH} ({len(ad_stats_rows)} строк).",
                    flush=True,
                )
                summary_lines = []
                for seller_id, rows_count in sorted(ad_stats_rows_by_seller.items(), key=lambda item: seller_brand_map.get(item[0], item[0])):
                    brand = seller_brand_map.get(seller_id, seller_id)
                    campaigns_with_data = len(ad_stats_campaigns_with_data.get(seller_id, set()))
                    empty_count = ad_stats_empty_campaigns.get(seller_id, 0)
                    summary_lines.append(
                        f"{brand} | rows:{rows_count} | кампаний:{campaigns_with_data} | пустых:{empty_count}"
                    )
                summary_text = html.escape("\n".join(summary_lines) if summary_lines else "нет данных")
                ad_stats_summary_message = (
                    "<b>05 WB API</b> | Ad Stats\n"
                    "<blockquote><b>Статистика собрана ✅</b>\n"
                    f"<code>{summary_text}</code></blockquote>"
                )
                send_to_telegram(ad_stats_summary_message)
            else:
                print("[WB-bot] Нет данных статистики по рекламным кампаниям.", flush=True)

            if ad_stats_errors:
                errors_text = html.escape("\n".join(ad_stats_errors))
                error_message = (
                    "<b>05 WB API</b> | Ad Stats\n"
                    "<blockquote><b>Ошибки при запросе</b>\n"
                    f"<code>{errors_text}</code></blockquote>"
                )
                send_to_telegram(error_message)

            ad_stats_db_loaded = False
            if ad_stats_rows:
                try:
                    load_ad_stats_into_db(cursor)
                    ad_stats_db_loaded = True
                    print("[WB-bot] WB_ad_stats успешно обновлена через LOAD DATA INFILE.", flush=True)
                    ad_stats_load_message = (
                        "<b>05 WB API</b> | Ad Stats\n"
                        "<blockquote>Данные загружены в таблицу WB_ad_stats.</blockquote>"
                    )
                    send_to_telegram(ad_stats_load_message)
                except Exception as db_err:
                    print(f"[WB-bot] Ошибка LOAD DATA для статистики реклам: {db_err}", file=sys.stderr, flush=True)
                    error_message = (
                        "<b>05 WB API</b> | Ad Stats\n"
                        "<blockquote>"
                        "Ошибка загрузки данных в WB_ad_stats.\n"
                        f"<code>{html.escape(str(db_err))}</code>"
                        "</blockquote>"
                    )
                    try:
                        send_to_telegram(error_message)
                    except Exception:
                        traceback.print_exc()

            if ad_stats_db_loaded:
                ad_stats_status_updates, ad_stats_status_details = update_ad_stats_status(
                    cursor,
                    ad_stats_processed,
                    ad_stats_rows,
                    ad_stats_requests_by_seller,
                    ad_stats_successful_requests_by_seller,
                    ad_stats_failed_requests_by_seller,
                )
                if ad_stats_status_updates:
                    connection.commit()
                    print(
                        f"[WB-bot] Обновлены ad_stats_status для {ad_stats_status_updates} селлеров.",
                        flush=True,
                    )
                    if ad_stats_status_details:
                        ad_stats_status_message = build_ad_stats_status_message(ad_stats_status_details)
                        send_to_telegram(ad_stats_status_message)
            else:
                print("[WB-bot] Статусы ad_stats не обновлялись из-за ошибки загрузки в БД.", flush=True)
        else:
            print("[WB-bot] Нет рекламных кампаний для запроса статистики.", flush=True)

        ad_stats_elapsed = time.time() - ad_stats_started_at
        ad_stats_completion_message = build_completion_message("05", "Ad Stats", ad_stats_elapsed)
        send_to_telegram(ad_stats_completion_message)
        time.sleep(2)

        ad_expenses_started_at = time.time()
        ad_expenses_sellers = fetch_ad_expenses_sellers(cursor_dict)
        ad_expenses_requests, ad_expenses_processed, ad_expenses_summary = prepare_ad_expenses_requests(ad_expenses_sellers)
        print("[WB-bot] Подготовлена выборка для списаний рекламных кампаний.", flush=True)
        ad_expenses_selection_message = build_ad_stats_selection_message(ad_expenses_summary)  # Используем ту же функцию для сообщения
        ad_expenses_selection_message = ad_expenses_selection_message.replace("05 WB API", "06 WB API").replace("Ad Stats", "Ad Expenses")
        send_to_telegram(ad_expenses_selection_message)

        ad_expenses_rows: List[Dict] = []
        ad_expenses_rows_by_seller: Dict[str, int] = defaultdict(int)
        ad_expenses_errors: List[str] = []
        seller_brand_map_expenses: Dict[str, str] = {}
        ad_expenses_requests_by_seller: Dict[str, List[Dict]] = defaultdict(list)
        ad_expenses_successful_requests_by_seller: Dict[str, List[Dict]] = defaultdict(list)
        ad_expenses_failed_requests_by_seller: Dict[str, List[Dict]] = defaultdict(list)

        if ad_expenses_requests:
            unique_sellers = len({req["seller_id"] for req in ad_expenses_requests})
            print(
                f"[WB-bot] Запланировано {len(ad_expenses_requests)} запросов списаний для {unique_sellers} селлеров.",
                flush=True,
            )

            total_requests = len(ad_expenses_requests)
            for request_index, req in enumerate(ad_expenses_requests, start=1):
                seller_id = req["seller_id"]
                brand = req["brand"]
                ad_expenses_requests_by_seller[seller_id].append(req)
                seller_brand_map_expenses[seller_id] = brand

                # Задержка между чанками
                if req.get("delay") == 1:
                    time.sleep(AD_EXPENSES_CHUNK_DELAY_SECONDS)

                print(
                    f"[WB-bot] Ad Expenses: запрос {request_index}/{total_requests} | "
                    f"{brand} | интервал {req['interval_index']}/{req['interval_total']} | "
                    f"{req['beginDate']}→{req['endDate']}",
                    flush=True,
                )

                # Задержка между запросами (1 запрос в секунду)
                if request_index > 1:
                    time.sleep(AD_EXPENSES_RATE_INTERVAL_SECONDS)

                params = {
                    "from": req["beginDate"],
                    "to": req["endDate"],
                }
                headers = {
                    "user-agent": WB_USER_AGENT,
                    "Authorization": f"Bearer {req['token']}",
                }

                success = False

                for attempt in range(1, AD_EXPENSES_MAX_RETRIES + 1):
                    try:
                        response = requests.get(WB_AD_EXPENSES_URL, params=params, headers=headers, timeout=40)
                        status_code = response.status_code
                        response_text = (response.text or "").strip()

                        if status_code == 200:
                            payload = response.json()
                            if isinstance(payload, list):
                                processed_rows = process_ad_expenses_response(payload, seller_id)
                                if processed_rows:
                                    ad_expenses_rows_by_seller[seller_id] += len(processed_rows)
                                    for row in processed_rows:
                                        row["brand"] = brand
                                        ad_expenses_rows.append(row)

                            ad_expenses_successful_requests_by_seller[seller_id].append(req)
                            success = True
                            break

                        if status_code in {429, 500}:
                            if attempt == AD_EXPENSES_MAX_RETRIES:
                                ad_expenses_failed_requests_by_seller[seller_id].append(req)
                                error_line = (
                                    f"{brand} | интервал {req['interval_index']}/{req['interval_total']} | "
                                    f"{req['beginDate']}→{req['endDate']} | "
                                    f"{status_code} {response_text}"
                                )
                                ad_expenses_errors.append(error_line)
                                print(
                                    f"[WB-bot] Ошибка WB Ad Expenses API: {error_line}",
                                    file=sys.stderr,
                                    flush=True,
                                )
                                break
                            delay = AD_EXPENSES_RETRY_BASE_DELAY_SECONDS * attempt
                            time.sleep(delay)
                            continue

                        ad_expenses_failed_requests_by_seller[seller_id].append(req)
                        error_line = (
                            f"{brand} | интервал {req['interval_index']}/{req['interval_total']} | "
                            f"{req['beginDate']}→{req['endDate']} | "
                            f"{status_code} {response_text}"
                        )
                        ad_expenses_errors.append(error_line)
                        print(
                            f"[WB-bot] Ошибка WB Ad Expenses API: {error_line}",
                            file=sys.stderr,
                            flush=True,
                        )
                        break
                    except Exception as api_err:
                        if attempt == AD_EXPENSES_MAX_RETRIES:
                            ad_expenses_failed_requests_by_seller[seller_id].append(req)
                            error_line = (
                                f"{brand} | интервал {req['interval_index']}/{req['interval_total']} | "
                                f"{req['beginDate']}→{req['endDate']} | {api_err}"
                            )
                            ad_expenses_errors.append(error_line)
                            print(
                                f"[WB-bot] Ошибка WB Ad Expenses API: {error_line}",
                                file=sys.stderr,
                                flush=True,
                            )
                            break
                        delay = AD_EXPENSES_RETRY_BASE_DELAY_SECONDS * attempt
                        time.sleep(delay)

                if not success:
                    continue

            if ad_expenses_rows:
                # Удаляем служебное поле brand перед записью в CSV
                csv_rows = [{k: v for k, v in row.items() if k in AD_EXPENSES_CSV_COLUMNS} for row in ad_expenses_rows]
                write_ad_expenses_csv(csv_rows, WB_AD_EXPENSES_CSV_HOST_PATH)
                print(
                    f"[WB-bot] CSV списаний реклам сформирован: {WB_AD_EXPENSES_CSV_HOST_PATH} ({len(ad_expenses_rows)} строк).",
                    flush=True,
                )
                summary_lines = []
                for seller_id, rows_count in sorted(ad_expenses_rows_by_seller.items(), key=lambda item: seller_brand_map_expenses.get(item[0], item[0])):
                    brand = seller_brand_map_expenses.get(seller_id, seller_id)
                    summary_lines.append(f"{brand} | rows:{rows_count}")
                summary_text = html.escape("\n".join(summary_lines) if summary_lines else "нет данных")
                ad_expenses_summary_message = (
                    "<b>06 WB API</b> | Ad Expenses\n"
                    "<blockquote><b>Списания собраны ✅</b>\n"
                    f"<code>{summary_text}</code></blockquote>"
                )
                send_to_telegram(ad_expenses_summary_message)
            else:
                print("[WB-bot] Нет данных по списаниям рекламных кампаний.", flush=True)

            if ad_expenses_errors:
                errors_text = html.escape("\n".join(ad_expenses_errors))
                error_message = (
                    "<b>06 WB API</b> | Ad Expenses\n"
                    "<blockquote><b>Ошибки при запросе</b>\n"
                    f"<code>{errors_text}</code></blockquote>"
                )
                send_to_telegram(error_message)

            ad_expenses_db_loaded = False
            if ad_expenses_rows:
                try:
                    load_ad_expenses_into_db(cursor)
                    ad_expenses_db_loaded = True
                    print("[WB-bot] WB_ad_expenses успешно обновлена через LOAD DATA INFILE.", flush=True)
                    ad_expenses_load_message = (
                        "<b>06 WB API</b> | Ad Expenses\n"
                        "<blockquote>Данные загружены в таблицу WB_ad_expenses.</blockquote>"
                    )
                    send_to_telegram(ad_expenses_load_message)
                except Exception as db_err:
                    print(f"[WB-bot] Ошибка LOAD DATA для списаний реклам: {db_err}", file=sys.stderr, flush=True)
                    error_message = (
                        "<b>06 WB API</b> | Ad Expenses\n"
                        "<blockquote>"
                        "Ошибка загрузки данных в WB_ad_expenses.\n"
                        f"<code>{html.escape(str(db_err))}</code>"
                        "</blockquote>"
                    )
                    try:
                        send_to_telegram(error_message)
                    except Exception:
                        traceback.print_exc()

            if ad_expenses_db_loaded:
                ad_expenses_status_updates, ad_expenses_status_details = update_ad_expenses_status(
                    cursor,
                    ad_expenses_processed,
                    ad_expenses_rows,
                    ad_expenses_requests_by_seller,
                    ad_expenses_successful_requests_by_seller,
                    ad_expenses_failed_requests_by_seller,
                )
                if ad_expenses_status_updates:
                    connection.commit()
                    print(
                        f"[WB-bot] Обновлены ad_expenses_status для {ad_expenses_status_updates} селлеров.",
                        flush=True,
                    )
                    if ad_expenses_status_details:
                        ad_expenses_status_message = build_ad_stats_status_message(ad_expenses_status_details)  # Используем ту же функцию
                        ad_expenses_status_message = ad_expenses_status_message.replace("05 WB API", "06 WB API").replace("Ad Stats", "Ad Expenses")
                        send_to_telegram(ad_expenses_status_message)
            else:
                print("[WB-bot] Статусы ad_expenses не обновлялись из-за ошибки загрузки в БД.", flush=True)
        else:
            print("[WB-bot] Нет селлеров для запроса списаний.", flush=True)

        ad_expenses_elapsed = time.time() - ad_expenses_started_at
        ad_expenses_completion_message = build_completion_message("06", "Ad Expenses", ad_expenses_elapsed)
        send_to_telegram(ad_expenses_completion_message)
        time.sleep(2)

        # 07 WB API | STOCKS
        stocks_started_at = time.time()
        stocks_sellers = fetch_stocks_sellers(cursor_dict)
        stocks_requests, stocks_processed, stocks_summary = prepare_stocks_requests(stocks_sellers)
        print("[WB-bot] Подготовлена выборка для остатков.", flush=True)
        stocks_selection_message = build_stocks_selection_message(stocks_summary)
        send_to_telegram(stocks_selection_message)

        stocks_rows: List[Dict] = []
        stocks_rows_by_seller: Dict[str, int] = defaultdict(int)
        stocks_errors: List[str] = []
        seller_brand_map_stocks: Dict[str, str] = {}
        stocks_api_error_sellers: Set[str] = set()
        stocks_successful_sellers: Set[str] = set()

        if stocks_requests:
            unique_sellers = len({req["seller_id"] for req in stocks_requests})
            print(
                f"[WB-bot] Запланировано {len(stocks_requests)} запросов остатков для {unique_sellers} селлеров.",
                flush=True,
            )

            total_requests = len(stocks_requests)
            for request_index, req in enumerate(stocks_requests, start=1):
                seller_id = req["seller_id"]
                brand = req["brand"]
                token = req["token"]
                seller_brand_map_stocks[seller_id] = brand

                print(
                    f"[WB-bot] Stocks: запрос {request_index}/{total_requests} | {brand}",
                    flush=True,
                )

                # Этап 1: Создание отчета
                task_id = None
                for attempt in range(1, STOCKS_MAX_RETRIES + 1):
                    try:
                        task_id = create_stocks_report(token)
                        if task_id:
                            break
                        if attempt < STOCKS_MAX_RETRIES:
                            delay = STOCKS_RETRY_BASE_DELAY_SECONDS * attempt
                            time.sleep(delay)
                    except Exception as api_err:
                        if attempt == STOCKS_MAX_RETRIES:
                            error_line = f"{brand} | создание отчета | {api_err}"
                            stocks_errors.append(error_line)
                            stocks_api_error_sellers.add(seller_id)
                            print(
                                f"[WB-bot] Ошибка WB Stocks API (создание отчета): {error_line}",
                                file=sys.stderr,
                                flush=True,
                            )
                            break
                        delay = STOCKS_RETRY_BASE_DELAY_SECONDS * attempt
                        time.sleep(delay)

                if not task_id:
                    stocks_api_error_sellers.add(seller_id)
                    continue

                # Этап 2: Ожидание готовности отчета
                status = None
                for check_index in range(1, STOCKS_MAX_STATUS_CHECKS + 1):
                    try:
                        status = check_stocks_status(task_id, token)
                        if status == "done":
                            break
                        if status == "error":
                            error_line = f"{brand} | статус отчета | error"
                            stocks_errors.append(error_line)
                            stocks_api_error_sellers.add(seller_id)
                            print(
                                f"[WB-bot] Ошибка WB Stocks API (статус отчета): {error_line}",
                                file=sys.stderr,
                                flush=True,
                            )
                            break
                        # Ждем перед следующей проверкой
                        time.sleep(STOCKS_STATUS_CHECK_INTERVAL_SECONDS)
                    except Exception as api_err:
                        if check_index == STOCKS_MAX_STATUS_CHECKS:
                            error_line = f"{brand} | проверка статуса | {api_err}"
                            stocks_errors.append(error_line)
                            stocks_api_error_sellers.add(seller_id)
                            print(
                                f"[WB-bot] Ошибка WB Stocks API (проверка статуса): {error_line}",
                                file=sys.stderr,
                                flush=True,
                            )
                            break
                        time.sleep(STOCKS_STATUS_CHECK_INTERVAL_SECONDS)

                if status != "done":
                    stocks_api_error_sellers.add(seller_id)
                    continue

                # Этап 3: Скачивание данных
                response_data = None
                for attempt in range(1, STOCKS_MAX_RETRIES + 1):
                    try:
                        response_data = download_stocks_data(task_id, token)
                        if response_data is not None:
                            break
                        if attempt < STOCKS_MAX_RETRIES:
                            delay = STOCKS_RETRY_BASE_DELAY_SECONDS * attempt
                            time.sleep(delay)
                    except Exception as api_err:
                        if attempt == STOCKS_MAX_RETRIES:
                            error_line = f"{brand} | скачивание данных | {api_err}"
                            stocks_errors.append(error_line)
                            stocks_api_error_sellers.add(seller_id)
                            print(
                                f"[WB-bot] Ошибка WB Stocks API (скачивание данных): {error_line}",
                                file=sys.stderr,
                                flush=True,
                            )
                            break
                        delay = STOCKS_RETRY_BASE_DELAY_SECONDS * attempt
                        time.sleep(delay)

                if response_data is None:
                    stocks_api_error_sellers.add(seller_id)
                    continue

                stocks_successful_sellers.add(seller_id)

                # Обработка данных
                processed_rows = process_stocks_response(response_data, seller_id)
                if processed_rows:
                    stocks_rows_by_seller[seller_id] += len(processed_rows)
                    for row in processed_rows:
                        row["brand"] = brand
                        stocks_rows.append(row)

            stocks_db_loaded = False
            if stocks_rows:
                # Удаляем поле 'brand' перед записью в CSV
                csv_rows = [{k: v for k, v in row.items() if k in STOCKS_CSV_COLUMNS} for row in stocks_rows]
                write_stocks_csv(csv_rows, WB_STOCKS_CSV_HOST_PATH)
                print(
                    f"[WB-bot] CSV остатков сформирован: {WB_STOCKS_CSV_HOST_PATH} ({len(stocks_rows)} строк).",
                    flush=True,
                )
                summary_lines = []
                for seller_id, rows_count in sorted(stocks_rows_by_seller.items(), key=lambda item: seller_brand_map_stocks.get(item[0], item[0])):
                    brand = seller_brand_map_stocks.get(seller_id, seller_id)
                    summary_lines.append(f"{brand} | rows:{rows_count}")
                summary_text = html.escape("\n".join(summary_lines) if summary_lines else "нет данных")
                stocks_summary_message = (
                    "<b>07 WB API</b> | STOCKS\n"
                    "<blockquote><b>Остатки собраны ✅</b>\n"
                    f"<code>{summary_text}</code></blockquote>"
                )
                send_to_telegram(stocks_summary_message)
                
                # Загрузка в БД
                try:
                    load_stocks_into_db(cursor)
                    stocks_db_loaded = True
                    print("[WB-bot] WB_stocks успешно обновлена через LOAD DATA INFILE.", flush=True)
                    stocks_load_message = (
                        "<b>07 WB API</b> | STOCKS\n"
                        "<blockquote>Данные загружены в таблицу WB_stocks.</blockquote>"
                    )
                    send_to_telegram(stocks_load_message)
                except Exception as db_err:
                    print(f"[WB-bot] Ошибка LOAD DATA для остатков: {db_err}", file=sys.stderr, flush=True)
                    error_message = (
                        "<b>07 WB API</b> | STOCKS\n"
                        "<blockquote>"
                        "Ошибка загрузки данных в WB_stocks.\n"
                        f"<code>{html.escape(str(db_err))}</code>"
                        "</blockquote>"
                    )
                    try:
                        send_to_telegram(error_message)
                    except Exception:
                        traceback.print_exc()
                if stocks_db_loaded:
                    stocks_status_updates, stocks_status_details = update_stocks_status(
                        cursor,
                        stocks_processed,
                        stocks_rows,
                        stocks_api_error_sellers,
                        stocks_successful_sellers,
                    )
                    if stocks_status_updates:
                        connection.commit()
                        print(
                            f"[WB-bot] Обновлены stock_status для {stocks_status_updates} селлеров.",
                            flush=True,
                        )
                        if stocks_status_details:
                            stocks_status_message = build_stocks_status_message(stocks_status_details)
                            send_to_telegram(stocks_status_message)
                else:
                    print("[WB-bot] Статусы stock_status не обновлялись из-за ошибки загрузки в БД.", flush=True)
            else:
                print("[WB-bot] Нет данных по остаткам.", flush=True)

            if stocks_errors:
                errors_text = html.escape("\n".join(stocks_errors))
                error_message = (
                    "<b>07 WB API</b> | STOCKS\n"
                    "<blockquote><b>Ошибки при запросе</b>\n"
                    f"<code>{errors_text}</code></blockquote>"
                )
                send_to_telegram(error_message)
        else:
            print("[WB-bot] Нет селлеров для запроса остатков.", flush=True)

        stocks_elapsed = time.time() - stocks_started_at
        stocks_completion_message = build_completion_message("07", "STOCKS", stocks_elapsed)
        send_to_telegram(stocks_completion_message)
        time.sleep(2)

        # 01 DB RNP | ReportDetail
        report_detail_started_at = time.time()
        reportdetail_start_msg_id: Optional[int] = None
        try:
            start_reply = send_to_telegram("<b>01 DB RNP | ReportDetail</b> | Запуск...")
            if isinstance(start_reply, dict):
                result = start_reply.get("result") or {}
                message_id = result.get("message_id")
                if isinstance(message_id, int):
                    reportdetail_start_msg_id = message_id
        except Exception:
            traceback.print_exc()

        report_orders_counts: Dict[str, int] = defaultdict(int)
        report_sales_counts: Dict[str, int] = defaultdict(int)
        report_errors: List[str] = []
        report_brand_map: Dict[str, str] = {}
        report_any_changes = False

        report_detail_orders_sellers = fetch_report_detail_sellers(cursor_dict)

        if report_detail_orders_sellers:
            print(
                f"[WB-bot] ReportDetail: подготовлено {len(report_detail_orders_sellers)} селлеров для этапа заказов.",
                flush=True,
            )
            for item in report_detail_orders_sellers:
                seller_id = str(item.get("seller_id") or "").strip()
                if not seller_id:
                    continue
                brand = item.get("brand") or seller_id
                report_brand_map.setdefault(seller_id, brand)
                try:
                    cursor.execute(REPORT_DETAIL_INSERT_ORDERS_SQL, (seller_id,))
                    affected = cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0
                    if affected:
                        report_any_changes = True
                        report_orders_counts[seller_id] += affected
                    print(
                        f"[WB-bot] ReportDetail: orders | {brand} | affected: {affected}",
                        flush=True,
                    )
                except Error as exc:
                    error_line = f"{brand} | orders | {exc}"
                    report_errors.append(error_line)
                    print(f"[WB-bot] Ошибка ReportDetail (orders): {error_line}", file=sys.stderr, flush=True)
        else:
            print("[WB-bot] ReportDetail: нет селлеров для этапа заказов.", flush=True)

        report_detail_sales_sellers = fetch_report_detail_sellers(cursor_dict)

        if report_detail_sales_sellers:
            print(
                f"[WB-bot] ReportDetail: подготовлено {len(report_detail_sales_sellers)} селлеров для этапа продаж.",
                flush=True,
            )
            for item in report_detail_sales_sellers:
                seller_id = str(item.get("seller_id") or "").strip()
                if not seller_id:
                    continue
                brand = item.get("brand") or seller_id
                report_brand_map.setdefault(seller_id, brand)
                try:
                    cursor.execute(REPORT_DETAIL_INSERT_SALES_SQL, (seller_id,))
                    affected = cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0
                    if affected:
                        report_any_changes = True
                        report_sales_counts[seller_id] += affected
                    print(
                        f"[WB-bot] ReportDetail: sales | {brand} | affected: {affected}",
                        flush=True,
                    )
                except Error as exc:
                    error_line = f"{brand} | sales | {exc}"
                    report_errors.append(error_line)
                    print(f"[WB-bot] Ошибка ReportDetail (sales): {error_line}", file=sys.stderr, flush=True)
        else:
            print("[WB-bot] ReportDetail: нет селлеров для этапа продаж.", flush=True)

        if report_any_changes:
            connection.commit()
        report_any_changes = False

        try:
            cursor.execute(REPORT_DETAIL_UPDATE_PURPRICE_SQL)
            purprice_affected = cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0
            if purprice_affected:
                connection.commit()
                report_any_changes = True
            print(
                f"[WB-bot] ReportDetail: обновлены себестоимость и логистика для {purprice_affected} записей.",
                flush=True,
            )
        except Error as exc:
            error_line = f"purprice update | {exc}"
            report_errors.append(error_line)
            print(f"[WB-bot] Ошибка ReportDetail (purprice): {error_line}", file=sys.stderr, flush=True)

        try:
            cursor.execute(REPORT_DETAIL_UPDATE_LOGISTICS_SQL)
            logistics_affected = cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0
            if logistics_affected:
                connection.commit()
                report_any_changes = True
            print(
                f"[WB-bot] ReportDetail: обновлены Logistics_return_cost для {logistics_affected} записей.",
                flush=True,
            )
        except Error as exc:
            error_line = f"logistics update | {exc}"
            report_errors.append(error_line)
            print(f"[WB-bot] Ошибка ReportDetail (logistics): {error_line}", file=sys.stderr, flush=True)

        def fmt_int(value: Any) -> str:
            try:
                number = int(value)
            except Exception:
                number = 0
            return f"{number:,}".replace(",", " ")

        summary_entries = []
        for seller_id in report_brand_map.keys():
            brand = report_brand_map.get(seller_id, seller_id)
            orders_cnt = report_orders_counts.get(seller_id, 0)
            sales_cnt = report_sales_counts.get(seller_id, 0)
            summary_entries.append(
                {
                    "brand": brand,
                    "orders": orders_cnt,
                    "sales": sales_cnt,
                }
            )

        summary_entries.sort(key=lambda item: (-item["orders"], (item["brand"] or "").lower()))

        summary_lines = [
            f"orders:{fmt_int(item['orders'])} | sales:{fmt_int(item['sales'])} | {item['brand']}"
            for item in summary_entries
        ]

        summary_body = "\n".join(html.escape(line) for line in summary_lines) if summary_lines else "нет данных"
        summary_message = (
            "<b>01 DB RNP | ReportDetail</b>\n\n"
            "Таблица обновлена ✅\n"
            f"<blockquote>{summary_body}</blockquote>"
        )

        edited_ok = False
        if reportdetail_start_msg_id is not None:
            try:
                edit_telegram_message(reportdetail_start_msg_id, summary_message)
                edited_ok = True
            except Exception:
                traceback.print_exc()

        if not edited_ok:
            send_to_telegram(summary_message)

        if report_errors:
            errors_text = html.escape("\n".join(report_errors))
            report_error_message = (
                "<b>01 DB RNP | ReportDetail</b>\n"
                "<blockquote><b>Ошибки при обновлении</b>\n"
                f"<code>{errors_text}</code></blockquote>"
            )
            send_to_telegram(report_error_message)

        report_detail_elapsed = time.time() - report_detail_started_at
        report_detail_completion_message = build_completion_message("01 DB RNP", "ReportDetail", report_detail_elapsed)
        send_to_telegram(report_detail_completion_message)
        time.sleep(2)

        # ⚡️ Сводная (TOTAL)
        total_started_at = time.time()
        total_start_msg_id: Optional[int] = None
        try:
            start_reply = send_to_telegram("<b>⚡️ Сводная</b> | Запуск...")
            if isinstance(start_reply, dict):
                result = start_reply.get("result") or {}
                message_id = result.get("message_id")
                if isinstance(message_id, int):
                    total_start_msg_id = message_id
        except Exception:
            traceback.print_exc()

        total_sellers = fetch_total_nmid_sellers(cursor_dict)
        total_allowed, total_processed, total_summary_text = prepare_total_nmid_requests(total_sellers)

        if total_processed:
            selection_body = html.escape(total_summary_text) if total_summary_text else "нет данных"
            selection_message = (
                "<b>⚡️ Сводная</b> | Отбор\n"
                f"<blockquote>{selection_body}</blockquote>"
            )
            send_to_telegram(selection_message)
        else:
            print("[WB-bot] Сводная: нет селлеров для обработки.", flush=True)

        total_outputs: List[Dict] = []
        total_errors: List[str] = []
        total_summary: Dict[str, Dict[str, Any]] = {}
        total_chunks = 0
        today_date = date.today()
        # Сохраняем пул артикулов для использования во втором цикле (stock_history)
        total_cards_by_seller: Dict[str, List[Dict]] = {}

        # Начальное сообщение со всеми этапами
        stage_msg_id: Optional[int] = None
        if total_allowed:
            initial_message = (
                "<b>⚡️ Сводная</b>\n"
                "<blockquote>▫️ 1. Подсчёт day_metrics для nmid\n"
                "▫️ 2. Подсчёт истории остатков\n"
                "▫️ 3. Подсчёт текущих остатков\n"
                "▫️ 4. Подсчёт воронки\n"
                "▫️ 5. Подсчет статистики РК</blockquote>"
            )
            stage_msg_reply = send_to_telegram(initial_message)
            if isinstance(stage_msg_reply, dict):
                result = stage_msg_reply.get("result") or {}
                stage_msg_id = result.get("message_id")

        for seller in total_allowed:
            seller_id = seller["seller_id"]
            brand = seller.get("brand") or seller_id
            total_summary.setdefault(seller_id, {"brand": brand, "cards": 0, "months": 0})

            try:
                cards = fetch_total_nmid_cards(cursor_dict, seller_id)
            except Exception as exc:
                error_line = f"{brand} | fetch_cards | {exc}"
                total_errors.append(error_line)
                print(f"[WB-bot] Ошибка TOTAL (fetch cards): {error_line}", file=sys.stderr, flush=True)
                continue

            if not cards:
                print(f"[WB-bot] Сводная: у {brand} нет карточек.", flush=True)
                continue

            # Сохраняем cards для использования во втором цикле (stock_history)
            total_cards_by_seller[seller_id] = cards

            # Оптимизация: собираем все чанки для всех артикулов, затем делаем один запрос по селлеру
            cards_with_chunks: List[Tuple[Dict, List[Dict]]] = []
            seller_min_date_from: Optional[str] = None
            seller_max_date_to: Optional[str] = None
            
            for card in cards:
                card_nmid = card.get("nmid") or card.get("nmID")
                if card_nmid is None:
                    continue
                card["seller_id"] = seller_id
                card["nmid"] = card_nmid
                card_meta = {
                    "seller_id": seller_id,
                    "nmid": card_nmid,
                    "wb_api_brand": card.get("wb_api_brand") or brand,
                    "subjectName": card.get("subjectName") or "",
                    "title": card.get("title") or "",
                    "photo": card.get("photo") or "",
                    "vendorCode": card.get("vendorCode") or "",
                    "supplierArticle": card.get("supplierArticle") or card.get("vendorCode") or "",
                }

                chunks = build_total_nmid_chunks_for_card(card, today_date)
                if not chunks:
                    continue
                total_summary[seller_id]["cards"] += 1
                cards_with_chunks.append((card_meta, chunks))
                
                # Определяем общий период для селлера
                for chunk in chunks:
                    date_from = chunk["date_from"]
                    date_to = chunk["date_to"]
                    if seller_min_date_from is None or date_from < seller_min_date_from:
                        seller_min_date_from = date_from
                    if seller_max_date_to is None or date_to > seller_max_date_to:
                        seller_max_date_to = date_to

            # Один запрос по селлеру за весь период (все артикулы сразу)
            if cards_with_chunks and seller_min_date_from and seller_max_date_to:
                try:
                    all_seller_rows = fetch_total_nmid_rows_by_seller(
                        cursor_dict, seller_id, seller_min_date_from, seller_max_date_to
                    )
                    print(
                        f"[WB-bot] TOTAL: выгружено {len(all_seller_rows)} строк для {brand} за период {seller_min_date_from}→{seller_max_date_to}",
                        flush=True,
                    )
                except Exception as exc:
                    error_line = f"{brand} | fetch_rows_by_seller {seller_min_date_from}→{seller_max_date_to} | {exc}"
                    total_errors.append(error_line)
                    print(f"[WB-bot] Ошибка TOTAL (fetch rows by seller): {error_line}", file=sys.stderr, flush=True)
                    continue
                
                # Группируем данные по nmID в Python
                rows_by_nmid: Dict[Any, List[Dict]] = {}
                for row in all_seller_rows:
                    row_nmid = row.get("nmID") or row.get("nmid") or row.get("nmId_sale")
                    if row_nmid:
                        if row_nmid not in rows_by_nmid:
                            rows_by_nmid[row_nmid] = []
                        rows_by_nmid[row_nmid].append(row)
                
                # Обрабатываем каждый артикул с его чанками
                for card_meta, chunks in cards_with_chunks:
                    card_nmid = card_meta["nmid"]
                    card_rows = rows_by_nmid.get(card_nmid, [])
                    
                    # Фильтруем строки по чанкам артикула
                    all_rows_for_card: List[Dict] = []
                    min_date_from: Optional[str] = None
                    max_date_to: Optional[str] = None
                    
                    for chunk in chunks:
                        date_from = chunk["date_from"]
                        date_to = chunk["date_to"]
                        if min_date_from is None or date_from < min_date_from:
                            min_date_from = date_from
                        if max_date_to is None or date_to > max_date_to:
                            max_date_to = date_to
                        
                        # Фильтруем строки по периоду чанка
                        for row in card_rows:
                            date_order = row.get("Date_order")
                            if date_order:
                                date_order_str = normalize_date_value(date_order)
                                # Сравниваем только дату (первые 10 символов YYYY-MM-DD)
                                date_order_ymd = date_order_str[:10] if len(date_order_str) >= 10 else date_order_str
                                if date_order_ymd >= date_from and date_order_ymd < date_to:
                                    all_rows_for_card.append(row)
                    
                    total_chunks += len(chunks)

                    # Агрегируем все строки разом (как в n8n)
                    if all_rows_for_card and min_date_from and max_date_to:
                        try:
                            aggregated = aggregate_total_nmid_rows(card_meta, all_rows_for_card, min_date_from, max_date_to)
                        except Exception as exc:
                            error_line = (
                                f"{brand} | nmID:{card_nmid} | aggregate | {exc}"
                            )
                            total_errors.append(error_line)
                            print(f"[WB-bot] Ошибка TOTAL (aggregate): {error_line}", file=sys.stderr, flush=True)
                            continue

                        if aggregated:
                            total_outputs.extend(aggregated)
                            total_summary[seller_id]["months"] += len(aggregated)

        total_db_loaded = False
        if total_outputs:
            write_total_nmid_csv(total_outputs, TOTAL_NMID_CSV_HOST_PATH)
            print(
                f"[WB-bot] TOTAL: сформирован CSV {TOTAL_NMID_CSV_HOST_PATH} ({len(total_outputs)} записей).",
                flush=True,
            )
            
            # Загрузка в БД
            try:
                load_total_nmid_into_db(cursor)
                connection.commit()
                total_db_loaded = True
                print("[WB-bot] GS_RNP_ReportDetail_daily_nmid успешно обновлена через LOAD DATA INFILE.", flush=True)
                
                # Редактируем сообщение о завершении первого этапа
                stage1_message = (
                    "<b>⚡️ Сводная</b>\n"
                    "<blockquote>☑️ 1. Подсчёт day_metrics для nmid\n"
                    "▫️ 2. Подсчёт истории остатков\n"
                    "▫️ 3. Подсчёт текущих остатков\n"
                    "▫️ 4. Подсчёт воронки\n"
                    "▫️ 5. Подсчет статистики РК</blockquote>"
                )
                if stage_msg_id is not None:
                    try:
                        edit_telegram_message(stage_msg_id, stage1_message)
                    except Exception:
                        pass
                else:
                    send_to_telegram(stage1_message)
                
                # Второй цикл: подсчёт истории остатков (stock_history) - оптимизированная версия
                if total_cards_by_seller:
                    stock_history_updated = 0
                    stock_history_errors: List[str] = []
                    all_aggregated: List[Dict] = []
                    
                    for seller_id, cards in total_cards_by_seller.items():
                        brand = total_summary.get(seller_id, {}).get("brand", seller_id)
                        
                        if not cards:
                            continue
                        
                        # Получаем paid_storage_status из первой карточки (они все одинаковые для селлера)
                        paid_storage_status = cards[0].get("paid_storage_status")
                        
                        # Определяем период для stock_history
                        date_from, date_to = get_stock_history_date_range(paid_storage_status, today_date)
                        
                        try:
                            # Выгружаем все строки WB_storage_fee по селлеру за период
                            storage_fee_rows = fetch_storage_fee_by_seller(
                                cursor_dict, seller_id, date_from, date_to
                            )
                            print(
                                f"[WB-bot] TOTAL: выгружено {len(storage_fee_rows)} строк WB_storage_fee для {brand} за период {date_from}→{date_to}",
                                flush=True,
                            )
                            
                            if not storage_fee_rows:
                                continue
                            
                            # Получаем существующие записи из GS_RNP_ReportDetail_daily_nmid
                            # (чтобы обновлять только те месяцы, где есть продажи)
                            cursor_dict.execute("""
                                SELECT seller_nmid_month_year_key, year, month
                                FROM GS_RNP_ReportDetail_daily_nmid
                                WHERE seller_id = %s
                            """, (seller_id,))
                            existing_records = {
                                row["seller_nmid_month_year_key"]: row
                                for row in cursor_dict.fetchall()
                            }
                            
                            # Агрегируем данные
                            aggregated = aggregate_stock_history_rows(storage_fee_rows, existing_records)
                            
                            if aggregated:
                                all_aggregated.extend(aggregated)
                                print(
                                    f"[WB-bot] TOTAL: агрегировано stock_history для {len(aggregated)} записей ({brand})",
                                    flush=True,
                                )
                        except Exception as exc:
                            error_line = f"{brand} | stock_history | {exc}"
                            stock_history_errors.append(error_line)
                            print(f"[WB-bot] Ошибка TOTAL (stock_history): {error_line}", file=sys.stderr, flush=True)
                    
                    # Записываем все данные в CSV и загружаем в БД одним запросом
                    if all_aggregated:
                        try:
                            write_stock_history_csv(all_aggregated, TOTAL_NMID_STOCK_HISTORY_CSV_HOST_PATH)
                            load_stock_history_into_db(cursor)
                            connection.commit()
                            stock_history_updated = len(all_aggregated)
                            print(f"[WB-bot] TOTAL: обновлено stock_history для {stock_history_updated} записей (всего).", flush=True)
                        except Exception as exc:
                            error_line = f"stock_history CSV/DB | {exc}"
                            stock_history_errors.append(error_line)
                            print(f"[WB-bot] Ошибка TOTAL (stock_history CSV/DB): {error_line}", file=sys.stderr, flush=True)
                    
                    if stock_history_errors:
                        errors_text = html.escape("\n".join(stock_history_errors))
                        stock_error_message = (
                            "<b>⚡️ Сводная</b>\n"
                            "<blockquote><b>Ошибки при обновлении stock_history</b>\n"
                            f"<code>{errors_text}</code></blockquote>"
                        )
                        send_to_telegram(stock_error_message)
                    
                    # Редактируем сообщение о завершении второго этапа
                    stage2_message = (
                        "<b>⚡️ Сводная</b>\n"
                        "<blockquote>☑️ 1. Подсчёт day_metrics для nmid\n"
                        "☑️ 2. Подсчёт истории остатков\n"
                        "▫️ 3. Подсчёт текущих остатков\n"
                        "▫️ 4. Подсчёт воронки\n"
                        "▫️ 5. Подсчет статистики РК</blockquote>"
                    )
                    if stage_msg_id is not None:
                        try:
                            edit_telegram_message(stage_msg_id, stage2_message)
                        except Exception:
                            pass
                    else:
                        send_to_telegram(stage2_message)
                    
                    # Третий цикл: подсчёт текущих остатков (stock_today)
                    stock_today_updated = 0
                    stock_today_errors: List[str] = []
                    
                    for seller_id, cards in total_cards_by_seller.items():
                        brand = total_summary.get(seller_id, {}).get("brand", seller_id)
                        
                        for card in cards:
                            card_nmid = card.get("nmid") or card.get("nmID")
                            if card_nmid is None:
                                continue
                            
                            try:
                                affected = update_stock_today_for_card(cursor, seller_id, card_nmid)
                                if affected > 0:
                                    stock_today_updated += affected
                            except Exception as exc:
                                error_line = f"{brand} | nmID:{card_nmid} | stock_today | {exc}"
                                stock_today_errors.append(error_line)
                                print(f"[WB-bot] Ошибка TOTAL (stock_today): {error_line}", file=sys.stderr, flush=True)
                    
                    if stock_today_updated > 0:
                        connection.commit()
                        print(f"[WB-bot] TOTAL: обновлено stock_today для {stock_today_updated} записей.", flush=True)
                    
                    if stock_today_errors:
                        errors_text = html.escape("\n".join(stock_today_errors))
                        stock_today_error_message = (
                            "<b>⚡️ Сводная</b>\n"
                            "<blockquote><b>Ошибки при обновлении stock_today</b>\n"
                            f"<code>{errors_text}</code></blockquote>"
                        )
                        send_to_telegram(stock_today_error_message)
                    
                    # Редактируем сообщение о завершении третьего этапа
                    stage3_message = (
                        "<b>⚡️ Сводная</b>\n"
                        "<blockquote>☑️ 1. Подсчёт day_metrics для nmid\n"
                        "☑️ 2. Подсчёт истории остатков\n"
                        "☑️ 3. Подсчёт текущих остатков\n"
                        "▫️ 4. Подсчёт воронки\n"
                        "▫️ 5. Подсчет статистики РК</blockquote>"
                    )
                    if stage_msg_id is not None:
                        try:
                            edit_telegram_message(stage_msg_id, stage3_message)
                        except Exception:
                            pass
                    else:
                        send_to_telegram(stage3_message)
                    
                    # Четвёртый цикл: подсчёт воронки (funnel_metrics)
                    funnel_metrics_updated = 0
                    funnel_metrics_errors: List[str] = []
                    
                    for seller_id, cards in total_cards_by_seller.items():
                        brand = total_summary.get(seller_id, {}).get("brand", seller_id)
                        
                        for card in cards:
                            card_nmid = card.get("nmid") or card.get("nmID")
                            if card_nmid is None:
                                continue
                            
                            try:
                                affected = update_funnel_metrics_for_card(cursor, seller_id, card_nmid)
                                if affected > 0:
                                    funnel_metrics_updated += affected
                            except Exception as exc:
                                error_line = f"{brand} | nmID:{card_nmid} | funnel_metrics | {exc}"
                                funnel_metrics_errors.append(error_line)
                                print(f"[WB-bot] Ошибка TOTAL (funnel_metrics): {error_line}", file=sys.stderr, flush=True)
                    
                    if funnel_metrics_updated > 0:
                        connection.commit()
                        print(f"[WB-bot] TOTAL: обновлено funnel_metrics для {funnel_metrics_updated} записей.", flush=True)
                    
                    if funnel_metrics_errors:
                        errors_text = html.escape("\n".join(funnel_metrics_errors))
                        funnel_error_message = (
                            "<b>⚡️ Сводная</b>\n"
                            "<blockquote><b>Ошибки при обновлении funnel_metrics</b>\n"
                            f"<code>{errors_text}</code></blockquote>"
                        )
                        send_to_telegram(funnel_error_message)
                    
                    # Редактируем сообщение о завершении четвёртого этапа
                    stage4_message = (
                        "<b>⚡️ Сводная</b>\n"
                        "<blockquote>☑️ 1. Подсчёт day_metrics для nmid\n"
                        "☑️ 2. Подсчёт истории остатков\n"
                        "☑️ 3. Подсчёт текущих остатков\n"
                        "☑️ 4. Подсчёт воронки\n"
                        "▫️ 5. Подсчет статистики РК</blockquote>"
                    )
                    if stage_msg_id is not None:
                        try:
                            edit_telegram_message(stage_msg_id, stage4_message)
                        except Exception:
                            pass
                    else:
                        send_to_telegram(stage4_message)
                    
                    # Пятый цикл: подсчёт статистики РК (ad_stats)
                    if total_cards_by_seller:
                        ad_stats_updated = 0
                        ad_stats_errors: List[str] = []
                        all_ad_stats_aggregated: List[Dict] = []
                        
                        for seller_id, cards in total_cards_by_seller.items():
                            brand = total_summary.get(seller_id, {}).get("brand", seller_id)
                            
                            if not cards:
                                continue
                            
                            # Получаем ad_stats_status из первой карточки (они все одинаковые для селлера)
                            ad_stats_status = cards[0].get("ad_stats_status")
                            
                            # Определяем период для ad_stats
                            date_from, date_to = get_ad_stats_date_range(ad_stats_status, today_date)
                            
                            try:
                                # Выгружаем все строки WB_ad_stats по селлеру за период
                                ad_stats_rows = fetch_ad_stats_by_seller(
                                    cursor_dict, seller_id, date_from, date_to
                                )
                                print(
                                    f"[WB-bot] TOTAL: выгружено {len(ad_stats_rows)} строк WB_ad_stats для {brand} за период {date_from}→{date_to}",
                                    flush=True,
                                )
                                
                                if not ad_stats_rows:
                                    continue
                                
                                # Агрегируем данные
                                agg_start = time.time()
                                aggregated = aggregate_ad_stats_rows(ad_stats_rows)
                                agg_elapsed = time.time() - agg_start
                                print(f"[WB-bot] TOTAL: агрегация ad_stats для {brand} заняла {agg_elapsed:.2f} сек ({len(ad_stats_rows)} строк → {len(aggregated) if aggregated else 0} записей)", flush=True)
                                
                                if aggregated:
                                    all_ad_stats_aggregated.extend(aggregated)
                                    print(
                                        f"[WB-bot] TOTAL: агрегировано ad_stats для {len(aggregated)} записей ({brand})",
                                        flush=True,
                                    )
                                else:
                                    print(
                                        f"[WB-bot] TOTAL: предупреждение - агрегация ad_stats вернула пустой результат для {brand} (входных строк: {len(ad_stats_rows)})",
                                        flush=True,
                                    )
                            except Exception as exc:
                                error_line = f"{brand} | ad_stats | {exc}"
                                ad_stats_errors.append(error_line)
                                print(f"[WB-bot] Ошибка TOTAL (ad_stats): {error_line}", file=sys.stderr, flush=True)
                        
                        # Записываем все данные в CSV и загружаем в БД одним запросом
                        if all_ad_stats_aggregated:
                            try:
                                # Фильтруем только валидные seller_id (из total_cards_by_seller)
                                valid_seller_ids = set(total_cards_by_seller.keys())
                                filtered_aggregated = [
                                    row for row in all_ad_stats_aggregated
                                    if row.get("seller_id") in valid_seller_ids
                                ]
                                if len(filtered_aggregated) < len(all_ad_stats_aggregated):
                                    skipped = len(all_ad_stats_aggregated) - len(filtered_aggregated)
                                    print(f"[WB-bot] TOTAL: пропущено {skipped} записей ad_stats с невалидными seller_id", flush=True)
                                
                                csv_start = time.time()
                                print(f"[WB-bot] TOTAL: записываем {len(filtered_aggregated)} записей ad_stats в CSV...", flush=True)
                                write_total_ad_stats_csv(filtered_aggregated, TOTAL_NMID_AD_STATS_CSV_HOST_PATH)
                                csv_elapsed = time.time() - csv_start
                                print(f"[WB-bot] TOTAL: CSV записан за {csv_elapsed:.2f} сек, загружаем в БД...", flush=True)
                                
                                db_start = time.time()
                                load_total_ad_stats_into_db(cursor)
                                db_load_elapsed = time.time() - db_start
                                print(f"[WB-bot] TOTAL: LOAD DATA INFILE занял {db_load_elapsed:.2f} сек", flush=True)
                                
                                commit_start = time.time()
                                connection.commit()
                                commit_elapsed = time.time() - commit_start
                                print(f"[WB-bot] TOTAL: COMMIT занял {commit_elapsed:.2f} сек", flush=True)
                                
                                ad_stats_updated = len(all_ad_stats_aggregated)
                                print(f"[WB-bot] TOTAL: обновлено ad_stats для {ad_stats_updated} записей (всего).", flush=True)
                            except Exception as exc:
                                error_line = f"ad_stats CSV/DB | {exc}"
                                ad_stats_errors.append(error_line)
                                print(f"[WB-bot] Ошибка TOTAL (ad_stats CSV/DB): {error_line}", file=sys.stderr, flush=True)
                                traceback.print_exc()
                        else:
                            print(f"[WB-bot] TOTAL: нет данных для агрегации ad_stats (all_ad_stats_aggregated пуст).", flush=True)
                        
                        if ad_stats_errors:
                            errors_text = html.escape("\n".join(ad_stats_errors))
                            ad_stats_error_message = (
                                "<b>⚡️ Сводная</b>\n"
                                "<blockquote><b>Ошибки при обновлении ad_stats</b>\n"
                                f"<code>{errors_text}</code></blockquote>"
                            )
                            send_to_telegram(ad_stats_error_message)
                        
                        # Редактируем сообщение о завершении пятого этапа
                        stage5_message = (
                            "<b>⚡️ Сводная</b>\n"
                            "<blockquote>☑️ 1. Подсчёт day_metrics для nmid\n"
                            "☑️ 2. Подсчёт истории остатков\n"
                            "☑️ 3. Подсчёт текущих остатков\n"
                            "☑️ 4. Подсчёт воронки\n"
                            "☑️ 5. Подсчет статистики РК</blockquote>"
                        )
                        if stage_msg_id is not None:
                            try:
                                edit_telegram_message(stage_msg_id, stage5_message)
                            except Exception:
                                pass
                        else:
                            send_to_telegram(stage5_message)
            except Exception as db_err:
                print(f"[WB-bot] Ошибка LOAD DATA для TOTAL: {db_err}", file=sys.stderr, flush=True)
                error_line = f"Ошибка загрузки данных в GS_RNP_ReportDetail_daily_nmid: {db_err}"
                total_errors.append(error_line)
                total_db_loaded = False

        def fmt_total_int(value: Any) -> str:
            try:
                number = int(value)
            except Exception:
                number = 0
            return f"{number:,}".replace(",", " ")

        summary_lines_total = []
        for seller_id, info in sorted(
            total_summary.items(),
            key=lambda item: (-item[1].get("cards", 0), item[1].get("brand", item[0]).lower()),
        ):
            summary_lines_total.append(
                f"cards:{fmt_total_int(info.get('cards', 0))} | months:{fmt_total_int(info.get('months', 0))} | {info.get('brand', seller_id)}"
            )

        summary_body_total = (
            "\n".join(html.escape(line) for line in summary_lines_total) if summary_lines_total else "нет данных"
        )
        total_summary_message = (
            "<b>⚡️ Сводная</b>\n\n"
            "Таблица обновлена ✅\n"
            f"<blockquote>{summary_body_total}</blockquote>"
        )

        edited_total = False
        if total_start_msg_id is not None:
            try:
                edit_telegram_message(total_start_msg_id, total_summary_message)
                edited_total = True
            except Exception:
                traceback.print_exc()

        if not edited_total:
            send_to_telegram(total_summary_message)

        if total_errors:
            errors_text = html.escape("\n".join(total_errors))
            total_error_message = (
                "<b>⚡️ Сводная</b>\n"
                "<blockquote><b>Ошибки при обработке</b>\n"
                f"<code>{errors_text}</code></blockquote>"
            )
            send_to_telegram(total_error_message)

        total_elapsed = time.time() - total_started_at
        total_completion_message = build_completion_message("TOTAL", "RNP | Сводная", total_elapsed)
        send_to_telegram(total_completion_message)
        time.sleep(2)

        elapsed = int(time.time() - workflow_started_at)
        minutes, seconds = divmod(elapsed, 60)
        final_message = (
            "<b>WB API</b> | Цикл завершен ✅\n"
            f"<blockquote>Общее время: {minutes} мин {seconds} сек</blockquote>"
        )
        send_to_telegram(final_message)

        time.sleep(300)
        return 0
    except Error as err:
        print(f"[WB-bot] DB error: {err}", file=sys.stderr, flush=True)
        msg = (
            "<b>WB_sellers_updates</b>\n"
            "<blockquote>"
            "Ошибка выполнения SQL.\n"
            f"<code>{err}</code>"
            "</blockquote>"
        )
        try:
            send_to_telegram(msg)
        except Exception:
            traceback.print_exc()
        time.sleep(60)
        return 1
    except Exception as err:
        tb = "".join(traceback.format_exception(err))
        print(f"[WB-bot] Unexpected error: {err}", file=sys.stderr, flush=True)
        msg = (
            "<b>WB_sellers_updates</b>\n"
            "<blockquote>"
            "Неизвестная ошибка.\n"
            f"<code>{err}</code>"
            "</blockquote>"
        )
        try:
            send_to_telegram(msg)
        except Exception:
            traceback.print_exc()
        sys.stderr.write(tb)
        return 1
    finally:
        if cursor is not None:
            cursor.close()
        if cursor_dict is not None:
            cursor_dict.close()
        if connection is not None and connection.is_connected():
            connection.close()


if __name__ == "__main__":
    raise SystemExit(main())
