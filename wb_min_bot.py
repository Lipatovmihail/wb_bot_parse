#!/usr/bin/env python3
"""
Мини-бот: выполняет синхронизацию WB_sellers → WB_sellers_updates
и отправляет статус в Telegram.
"""

import html
import json
import os
import sys
import time
import traceback
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional, Set, Tuple

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
WB_CSV_HOST_PATH = "/data/csv/WB_orders_import.csv"
WB_CSV_MYSQL_PATH = "/var/lib/mysql-files/csv/WB_orders_import.csv"
WB_SALES_CSV_HOST_PATH = "/data/csv/WB_sales_import.csv"
WB_SALES_CSV_MYSQL_PATH = "/var/lib/mysql-files/csv/WB_sales_import.csv"
RIGHT_COOLDOWN_MINUTES = int(os.environ.get("WB_RIGHT_COOLDOWN_MINUTES", "30"))


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


def send_to_telegram(message: str) -> None:
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
        "disable_notification": True,
    }
    if TELEGRAM_THREAD_ID:
        payload["message_thread_id"] = TELEGRAM_THREAD_ID

    response = requests.post(
        f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
        json=payload,
        timeout=10,
    )
    response.raise_for_status()


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
    message = (
        "<b>Запускаю обновление  РНП</b>\n"
        f"<blockquote><b>В работу взяты {len(brand_lines)} брендов:</b>\n"
        f"{lines_joined}\n"
        "</blockquote>"
    )
    return message


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

        if (bucket.get("api_error") or bucket["has_error"]) and prev:
            orders_status = prev
        elif count == 0:
            if prev:
                orders_status = prev
            else:
                continue
        else:
            left_boundary = None
            right_boundary = None
            for r in rows:
                ymd_val = first_ymd(r.get("date")) or first_ymd(r.get("lastChangeDate"))
                if ymd_val:
                    left_boundary = min_ymd(left_boundary, ymd_val)
                    right_boundary = max_ymd(right_boundary, ymd_val)

            current_date_from = bucket["date_from"]
            prev_status = (prev or {}).get("status")
            prev_max_date_from = first_ymd((prev or {}).get("maxDateFrom"))

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

        if bucket.get("api_error") and prev:
            sales_status = prev
        elif count == 0:
            if prev:
                sales_status = prev
            else:
                continue
        else:
            left_boundary = None
            right_boundary = None
            for r in rows:
                ymd_val = first_ymd(r.get("date")) or first_ymd(r.get("lastChangeDate"))
                if ymd_val:
                    left_boundary = min_ymd(left_boundary, ymd_val)
                    right_boundary = max_ymd(right_boundary, ymd_val)

            current_date_from = bucket["date_from"]
            prev_status = (prev or {}).get("status")
            prev_max_date_from = first_ymd((prev or {}).get("maxDateFrom"))

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
        else:
            print("[WB-bot] Нет данных по продажам или все запросы завершились ошибкой.", flush=True)

        if not sales_data and sales_api_errors:
            print("[WB-bot] Статусы продаж не обновлялись из-за ошибок API.", flush=True)

        elapsed = int(time.time() - workflow_started_at)
        minutes, seconds = divmod(elapsed, 60)
        final_message = (
            "<b>01 WB API</b> | Orders & Sales завершены ✅\n"
            f"<blockquote>Время: {minutes} мин {seconds} сек</blockquote>"
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
