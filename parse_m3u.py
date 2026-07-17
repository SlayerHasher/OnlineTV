import asyncio
import aiohttp
import json
import re
import logging
import os
import sys
from collections import defaultdict
from urllib.parse import urlparse
from datetime import datetime
from typing import List, Dict, Optional
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# ---------- Настройка логирования ----------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('parser.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# ---------- Загрузка конфигурации ----------
with open('config.json', 'r', encoding='utf-8') as f:
    CONFIG = json.load(f)

SOURCES_FILE = CONFIG.get('sources_file', 'play.list')
OUTPUT_FILE = CONFIG.get('output_file', 'playlist.m3u')
STATS_FILE = CONFIG.get('stats_file', 'stats.json')
TIMEOUT = CONFIG.get('timeout', 30)
RETRIES = CONFIG.get('retries', 3)
CONCURRENT = CONFIG.get('concurrent_requests', 20)   # увеличен для скорости
CHECK_STREAM = CONFIG.get('check_stream', True)      # если True – HTTP-проверка
PREFERRED = CONFIG.get('prefered_sources', [])
EXCLUDE_KEYWORDS = CONFIG.get('exclude_keywords', [])
KEEP_GROUP = CONFIG.get('keep_group_title', True)
SORT_BY_GROUP = CONFIG.get('sort_by_group', True)

# ---------- Вспомогательные функции ----------
def normalize_name(name: str) -> str:
    """Нормализует название канала для сравнения."""
    name = name.lower().strip()
    name = re.sub(r'\s+', ' ', name)
    name = re.sub(r'[^\w\s]', '', name)
    return name

def extract_source_name(url: str) -> str:
    parsed = urlparse(url)
    return parsed.netloc.split('.')[0] if parsed.netloc else 'unknown'

# ---------- Загрузчик с повторными попытками ----------
@retry(
    stop=stop_after_attempt(RETRIES),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError))
)
async def fetch_playlist(session: aiohttp.ClientSession, url: str) -> str:
    logger.info(f"Загрузка: {url}")
    async with session.get(url, timeout=TIMEOUT) as resp:
        resp.raise_for_status()
        return await resp.text()

# ---------- Парсинг M3U ----------
def parse_m3u(content: str, source_url: str) -> List[Dict]:
    channels = []
    lines = content.splitlines()
    current = None
    for line in lines:
        line = line.strip()
        if not line:
            continue
        if line.startswith('#EXTINF:'):
            attrs = {}
            for attr in ['tvg-logo', 'group-title', 'tvg-id', 'tvg-name']:
                match = re.search(rf'{attr}="([^"]*)"', line)
                if match:
                    attrs[attr] = match.group(1)
            if ',' in line:
                name = line.split(',', 1)[1].strip()
            else:
                name = f"Channel {len(channels)}"
            current = {
                'name': name,
                'url': None,
                'source': source_url,
                'attrs': attrs,
                'valid': False,
            }
        elif current and not line.startswith('#') and line.startswith('http'):
            current['url'] = line
            channels.append(current)
            current = None
    return channels

# ---------- Быстрая HTTP-проверка (HEAD) ----------
async def http_check(url: str, timeout: int = 5) -> bool:
    """Проверяет доступность URL через HEAD-запрос."""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.head(url, timeout=timeout) as resp:
                return resp.status < 400
    except Exception:
        return False

# ---------- Основной сборщик ----------
async def main():
    logger.info("Запуск парсера IPTV (без ffprobe)...")
    start_time = datetime.now()

    # Читаем источники
    if not os.path.exists(SOURCES_FILE):
        logger.error(f"Файл {SOURCES_FILE} не найден!")
        sys.exit(1)
    with open(SOURCES_FILE, 'r', encoding='utf-8') as f:
        source_urls = [line.strip() for line in f if line.strip() and not line.startswith('#')]

    # Асинхронно загружаем все плейлисты
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_playlist(session, url) for url in source_urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    # Парсим и собираем каналы
    raw_channels = []
    source_stats = defaultdict(lambda: {'loaded': 0, 'parsed': 0, 'valid': 0})
    for url, result in zip(source_urls, results):
        source_name = extract_source_name(url)
        source_stats[source_name]['loaded'] = 1
        if isinstance(result, Exception):
            logger.error(f"Ошибка загрузки {url}: {result}")
            continue
        channels = parse_m3u(result, url)
        source_stats[source_name]['parsed'] = len(channels)
        raw_channels.extend(channels)

    logger.info(f"Всего каналов до фильтрации: {len(raw_channels)}")

    # Исключаем по ключевым словам
    filtered = [ch for ch in raw_channels if not any(kw in ch['name'].lower() for kw in EXCLUDE_KEYWORDS)]
    logger.info(f"После исключения ключевых слов: {len(filtered)}")

    # Дедупликация: группировка по нормализованному названию
    groups = defaultdict(list)
    for ch in filtered:
        groups[normalize_name(ch['name'])].append(ch)

    # Выбираем лучшего кандидата из каждой группы
    def sort_key(ch):
        source = extract_source_name(ch['source'])
        pref_score = 0
        for i, pref in enumerate(PREFERRED):
            if pref in source:
                pref_score = len(PREFERRED) - i
                break
        return (pref_score, ch['name'])

    candidates = [max(ch_list, key=sort_key) for ch_list in groups.values()]
    logger.info(f"Кандидатов после дедупликации: {len(candidates)}")

    # ---------- Валидация через HTTP (если включена) ----------
    final_channels = []
    if CHECK_STREAM:
        logger.info("Проверка доступности каналов через HTTP HEAD...")
        sem = asyncio.Semaphore(CONCURRENT)
        async def check_one(ch):
            async with sem:
                ch['valid'] = await http_check(ch['url'], timeout=5)
                return ch
        tasks = [check_one(ch) for ch in candidates]
        results = await asyncio.gather(*tasks)
        final_channels = [ch for ch in results if ch['valid']]
        logger.info(f"После HTTP-проверки осталось: {len(final_channels)}")
    else:
        final_channels = candidates

    # Сортировка по группам (если включено)
    if SORT_BY_GROUP:
        final_channels.sort(key=lambda ch: (ch['attrs'].get('group-title', ''), ch['name']))

    # Генерация M3U
    m3u_lines = ['#EXTM3U']
    for ch in final_channels:
        attrs = ch['attrs']
        extinf = f'#EXTINF:-1'
        if KEEP_GROUP and attrs.get('group-title'):
            extinf += f' group-title="{attrs["group-title"]}"'
        if attrs.get('tvg-logo'):
            extinf += f' tvg-logo="{attrs["tvg-logo"]}"'
        if attrs.get('tvg-id'):
            extinf += f' tvg-id="{attrs["tvg-id"]}"'
        extinf += f',{ch["name"]}'
        m3u_lines.append(extinf)
        m3u_lines.append(ch['url'])

    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.write('\n'.join(m3u_lines))

    # Статистика
    stats = {
        'generated': datetime.now().isoformat(),
        'sources': dict(source_stats),
        'raw_count': len(raw_channels),
        'after_filter': len(filtered),
        'after_dedup': len(candidates),
        'after_validation': len(final_channels),
        'final_count': len(final_channels),
        'duration_seconds': (datetime.now() - start_time).total_seconds()
    }
    with open(STATS_FILE, 'w', encoding='utf-8') as f:
        json.dump(stats, f, indent=2, ensure_ascii=False)

    logger.info(f" Готово! Плейлист сохранён в {OUTPUT_FILE}")
    logger.info(f" Статистика: {stats}")

if __name__ == '__main__':
    asyncio.run(main())