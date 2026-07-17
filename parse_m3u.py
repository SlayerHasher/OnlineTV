#!/usr/bin/env python3
"""
OnlineTV - M3U плейлист агрегатор
"""

import json
import os
import sys
import re
import asyncio
import hashlib
from datetime import datetime
from typing import Dict, List, Tuple

import requests
import aiohttp

CONFIG_FILE = "config.json"
PLAYLIST_OUTPUT = "playlist.m3u"
STATS_OUTPUT = "stats.json"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Accept": "*/*",
}

# Валидные Content-Type для IPTV стримов
VALID_CONTENT_TYPES = [
    "video/",
    "audio/",
    "mpegurl",
    "m3u8",
    "mpeg",
    "mp2t",
    "mp4",
    "flv",
    "x-mpegurl",
    "vnd.apple.mpegurl",
    "octet-stream",
    "application/json",  # некоторые API
]


def load_config() -> dict:
    if not os.path.exists(CONFIG_FILE):
        return {"sources": [], "output": PLAYLIST_OUTPUT, "check_streams": False}
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def download(url: str, timeout: int = 30, retries: int = 3) -> str:
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
            resp.raise_for_status()
            for enc in ("utf-8", "cp1251", "latin-1"):
                try:
                    return resp.content.decode(enc)
                except UnicodeDecodeError:
                    continue
            return resp.content.decode("utf-8", errors="ignore")
        except Exception as e:
            if attempt == retries - 1:
                print(f" Ошибка {url}: {e}")
                return ""
    return ""


def parse_extinf(line: str) -> Dict[str, str]:
    """Парсит #EXTINF строку в словарь атрибутов."""
    attrs = {}
    for match in re.finditer(r'([a-zA-Z0-9_-]+)="([^"]*)"', line):
        attrs[match.group(1).lower()] = match.group(2)
    
    match = re.match(r'#EXTINF:([^,]*),?(.*)', line)
    if match:
        attrs["duration"] = match.group(1).strip()
        attrs["name"] = match.group(2).strip()
    
    return attrs


def build_extinf(attrs: Dict[str, str]) -> str:
    """Собирает EXTINF обратно из словаря."""
    duration = attrs.get("duration", "-1")
    name = attrs.get("name", "Unknown")
    
    parts = [f"#EXTINF:{duration}"]
    skip = {"duration", "name"}
    for key, value in attrs.items():
        if key not in skip and value:
            parts.append(f'{key}="{value}"')
    
    return f"{' '.join(parts)},{name}"


def parse_playlist(content: str, source_name: str = "") -> List[Dict]:
    if not content:
        return []
    
    lines = [ln.rstrip("\r\n") for ln in content.splitlines()]
    entries = []
    
    current_extopts = []
    current_extinf = None
    i = 0
    
    while i < len(lines):
        line = lines[i].strip()
        
        if not line or line.upper() == "#EXTM3U":
            i += 1
            continue
        
        if line.startswith("#EXTVLCOPT") or line.startswith("#KODIPROP") or line.startswith("#EXTGRP"):
            current_extopts.append(line)
            i += 1
            continue
        
        if line.startswith("#EXTINF"):
            current_extinf = parse_extinf(line)
            if not current_extinf.get("name"):
                current_extinf["name"] = source_name
            i += 1
            continue
        
        if line.startswith(("http://", "https://", "rtmp://", "rtsp://")):
            if current_extinf is None:
                current_extinf = {"duration": "-1", "name": source_name}
            
            entries.append({
                "attrs": current_extinf,
                "extopts": current_extopts,
                "url": line,
            })
            current_extinf = None
            current_extopts = []
        
        i += 1
    
    return entries


def is_russian(entry: Dict) -> bool:
    """Проверяет, русский ли канал."""
    attrs = entry["attrs"]
    
    lang = attrs.get("tvg-language", "").lower()
    if lang and any(ru in lang for ru in ["ru", "рус", "russian"]):
        return True
    
    country = attrs.get("tvg-country", "").lower()
    if country and any(ru in country for ru in ["ru", "rus", "росс"]):
        return True
    
    name = attrs.get("name", "")
    if any("\u0400" <= c <= "\u04FF" for c in name):
        return True
    
    group = attrs.get("group-title", "")
    if any("\u0400" <= c <= "\u04FF" for c in group):
        return True
    
    return False


def deduplicate(entries: List[Dict]) -> List[Dict]:
    seen = set()
    result = []
    for e in entries:
        name = e["attrs"].get("name", "").lower().strip()
        key = (e["url"], name)
        if key not in seen:
            seen.add(key)
            result.append(e)
    return result


def is_valid_stream_response(status: int, content_type: str) -> bool:
    """Проверяет, является ли ответ валидным для IPTV стрима."""
    if not (200 <= status < 400):
        return False
    
    if not content_type:
        return True  # если Content-Type не указан — считаем OK
    
    ct = content_type.lower()
    return any(valid in ct for valid in VALID_CONTENT_TYPES)


async def check_stream(session: aiohttp.ClientSession, url: str, timeout: int) -> Tuple[str, bool, str]:
    """
    Проверяет стрим. Возвращает (url, is_ok, reason).
    Стратегия:
    1. HEAD запрос (быстро)
    2. Если не сработало — GET с чтением первых байт
    """
    # Пропускаем не-HTTP(S) URL без проверки
    if not url.startswith(("http://", "https://")):
        return url, True, "non-http"
    
    # Попытка 1: HEAD запрос
    try:
        async with session.head(
            url, 
            timeout=aiohttp.ClientTimeout(total=timeout),
            allow_redirects=True, 
            ssl=False,
            headers=HEADERS,
        ) as resp:
            ct = resp.headers.get("Content-Type", "")
            if is_valid_stream_response(resp.status, ct):
                return url, True, f"head-{resp.status}"
    except Exception:
        pass
    
    # Попытка 2: GET запрос с чтением первых байт
    try:
        async with session.get(
            url,
            timeout=aiohttp.ClientTimeout(total=timeout),
            allow_redirects=True,
            ssl=False,
            headers=HEADERS,
        ) as resp:
            # Читаем первые 4KB чтобы убедиться что стрим живой
            await resp.content.read(4096)
            ct = resp.headers.get("Content-Type", "")
            if is_valid_stream_response(resp.status, ct):
                return url, True, f"get-{resp.status}"
            return url, False, f"bad-ct-{resp.status}-{ct[:20]}"
    except asyncio.TimeoutError:
        return url, False, "timeout"
    except aiohttp.ClientConnectorError:
        return url, False, "conn-err"
    except Exception as e:
        return url, False, f"err-{type(e).__name__}"


async def check_streams_batch(urls: List[str], timeout: int, workers: int) -> Dict[str, Tuple[bool, str]]:
    connector = aiohttp.TCPConnector(
        limit=workers, 
        limit_per_host=15, 
        ssl=False,
        force_close=True,
    )
    async with aiohttp.ClientSession(connector=connector, headers=HEADERS) as session:
        tasks = [check_stream(session, url, timeout) for url in set(urls)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
    
    out = {}
    for r in results:
        if isinstance(r, tuple) and len(r) == 3:
            out[r[0]] = (r[1], r[2])
    return out


def write_playlist(entries: List[Dict], path: str) -> bool:
    entries_sorted = sorted(
        entries,
        key=lambda e: (
            e["attrs"].get("group-title", "") or "",
            e["attrs"].get("name", "").lower()
        )
    )
    
    parts = ["#EXTM3U"]
    for e in entries_sorted:
        for opt in e["extopts"]:
            parts.append(opt)
        parts.append(build_extinf(e["attrs"]))
        parts.append(e["url"])
    
    content = "\n".join(parts) + "\n"
    new_hash = hashlib.md5(content.encode("utf-8")).hexdigest()
    
    if os.path.exists(path):
        with open(path, "rb") as f:
            old_hash = hashlib.md5(f.read()).hexdigest()
        if new_hash == old_hash:
            print(" Плейлист не изменился")
            return False
    
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)
    print(f" Записано: {path} ({len(entries)} каналов)")
    return True


def main() -> int:
    start = datetime.now()
    print(f" Запуск: {start.isoformat()}")
    
    config = load_config()
    sources = config.get("sources", [])
    output = config.get("output", PLAYLIST_OUTPUT)
    do_check = config.get("check_streams", False)
    timeout = config.get("check_timeout", 10)  # Увеличен с 6 до 10
    workers = config.get("check_workers", 30)  # Уменьшен с 50 до 30
    keep_failed = config.get("keep_failed", False)
    filter_ru = config.get("filter_russian", False)
    
    # Сбор всех каналов
    all_entries = []
    per_source = {}
    
    for src in sources:
        if not src.get("enabled", True):
            continue
        name = src.get("name", "Unknown")
        url = src.get("url", "")
        
        print(f" [{name}]")
        content = download(url)
        entries = parse_playlist(content, source_name=name)
        all_entries.extend(entries)
        per_source[name] = len(entries)
        print(f"   ✓ {len(entries)} каналов")
    
    print(f"\n Всего: {len(all_entries)}")
    
    # Дедупликация
    before_dedup = len(all_entries)
    all_entries = deduplicate(all_entries)
    print(f" После дедупликации: {len(all_entries)} (-{before_dedup - len(all_entries)})")
    
    # Фильтр русских
    if filter_ru:
        before_filter = len(all_entries)
        all_entries = [e for e in all_entries if is_russian(e)]
        print(f" Только русские: {len(all_entries)} (-{before_filter - len(all_entries)})")
    
    # Категории (оригинальные из плейлистов)
    categories = {}
    for e in all_entries:
        cat = e["attrs"].get("group-title", "") or "Без категории"
        categories[cat] = categories.get(cat, 0) + 1
    
    print(f"\n Категории ({len(categories)}):")
    for cat, count in sorted(categories.items(), key=lambda x: -x[1])[:20]:
        print(f"   {cat}: {count}")
    
    # Проверка стримов
    check_stats = {"total": 0, "ok": 0, "failed": 0, "reasons": {}}
    if do_check and all_entries:
        urls = [e["url"] for e in all_entries]
        print(f"\n Проверка {len(set(urls))} стримов ({workers} потоков, таймаут {timeout}с)...")
        results = asyncio.run(check_streams_batch(urls, timeout, workers))
        
        check_stats["total"] = len(results)
        for e in all_entries:
            ok, reason = results.get(e["url"], (False, "unknown"))
            e["_ok"] = ok
            if ok:
                check_stats["ok"] += 1
            else:
                check_stats["failed"] += 1
                check_stats["reasons"][reason] = check_stats["reasons"].get(reason, 0) + 1
        
        print(f" OK: {check_stats['ok']} |  FAIL: {check_stats['failed']}")
        print(f" Причины ошибок:")
        for reason, count in sorted(check_stats["reasons"].items(), key=lambda x: -x[1])[:10]:
            print(f"   {reason}: {count}")
        
        if not keep_failed:
            all_entries = [e for e in all_entries if e.get("_ok", True)]
            print(f" Итог: {len(all_entries)} каналов")
    
    # Запись
    changed = write_playlist(all_entries, output)
    
    # Статистика
    stats = {
        "last_run": start.isoformat(),
        "duration_sec": round((datetime.now() - start).total_seconds(), 1),
        "sources": per_source,
        "categories": categories,
        "check": check_stats,
        "final": len(all_entries),
    }
    with open(STATS_OUTPUT, "w", encoding="utf-8") as f:
        json.dump(stats, f, indent=2, ensure_ascii=False)
    print(f"\n Статистика: {STATS_OUTPUT}")
    
    # ВСЕГДА возвращаем 0 - коммит делается через git diff в workflow
    return 0


if __name__ == "__main__":
    sys.exit(main())