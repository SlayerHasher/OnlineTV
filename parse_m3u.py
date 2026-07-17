import asyncio
import aiohttp
import json
import re
import logging
import os
import subprocess
import sys
from collections import defaultdict
from urllib.parse import urlparse
from datetime import datetime
from typing import List, Dict, Optional, Tuple
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
CONCURRENT = CONFIG.get('concurrent_requests', 10)
CHECK_STREAM = CONFIG.get('check_stream', True)
MIN_DURATION = CONFIG.get('min_duration_seconds', 3)
FFPROBE_PATH = CONFIG.get('ffprobe_path', 'ffprobe')
PREFERRED = CONFIG.get('prefered_sources', [])
EXCLUDE_KEYWORDS = CONFIG.get('exclude_keywords', [])
KEEP_GROUP = CONFIG.get('keep_group_title', True)
SORT_BY_GROUP = CONFIG.get('sort_by_group', True)

# ---------- Вспомогательные функции ----------
def normalize_name(name: str) -> str:
    """Нормализует название канала для сравнения."""
    name = name.lower().strip()
    name = re.sub(r'\s+', ' ', name)          # убираем лишние пробелы
    name = re.sub(r'[^\w\s]', '', name)       # убираем спецсимволы
    return name

def extract_source_name(url: str) -> str:
    """Извлекает имя источника из URL (для статистики)."""
    parsed = urlparse(url)
    return parsed.netloc.split('.')[0] if parsed.netloc else 'unknown'

# ---------- Загрузчик с повторными попытками ----------
@retry(
    stop=stop_after_attempt(RETRIES),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError))
)
async def fetch_playlist(session: aiohttp.ClientSession, url: str) -> str:
    """Асинхронно загружает плейлист с ретраями."""
    logger.info(f"Загрузка: {url}")
    async with session.get(url, timeout=TIMEOUT) as resp:
        resp.raise_for_status()
        return await resp.text()

# ---------- Парсинг M3U ----------
def parse_m3u(content: str, source_url: str) -> List[Dict]:
    """Парсит M3U и возвращает список словарей с каналами."""
    channels = []
    lines = content.splitlines()
    current = None
    for line in lines:
        line = line.strip()
        if not line:
            continue
        if line.startswith('#EXTINF:'):
            # Извлекаем атрибуты
            attrs = {}
            for attr in ['tvg-logo', 'group-title', 'tvg-id', 'tvg-name']:
                match = re.search(rf'{attr}="([^"]*)"', line)
                if match:
                    attrs[attr] = match.group(1)
            # Извлекаем название (после запятой)
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
                'duration': None
            }
        elif current and not line.startswith('#') and line.startswith('http'):
            current['url'] = line
            channels.append(current)
            current = None
    return channels

# ---------- Быстрая HTTP-проверка доступности ----------
async def http_quick_check(url: str, timeout: int = 5) -> bool:
    """Проверяет, отвечает ли URL по HTTP (HEAD-запрос)."""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.head(url, timeout=timeout) as resp:
                return resp.status < 400
    except Exception:
        return False

# ---------- Глубокая валидация через ffprobe ----------
async def validate_stream_deep(url: str, min_duration: int = 3) -> Tuple[bool, Optional[float]]:
    """Проверяет, что поток содержит видео и длится хотя бы min_duration секунд."""
    if not os.path.exists(FFPROBE_PATH) and not os.system(f'which {FFPROBE_PATH} > /dev/null 2>&1') == 0:
        logger.warning("ffprobe не найден, проверка потока отключена")
        return True, None
    try:
        cmd = [
            FFPROBE_PATH,
            '-v', 'error',
            '-select_streams', 'v:0',
            '-show_entries', 'stream=codec_name',
            '-show_entries', 'format=duration',
            '-of', 'default=noprint_wrappers=1:nokey=1',
            url
        ]
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            return False, None
        output = stdout.decode().strip().split('\n')
        # Проверяем наличие видео
        has_video = any(line for line in output if line and 'codec_name' in line)
        if not has_video:
            return False, None
        # Проверяем длительность
        duration = None
        for line in output:
            if line and line.replace('.', '').isdigit():
                duration = float(line)
                break
        if duration is not None and duration < min_duration:
            return False, duration
        return True, duration
    except Exception as e:
        logger.error(f"Ошибка проверки {url}: {e}")
        return False, None

# ---------- Основной сборщик ----------
async def main():
    logger.info("Запуск парсера IPTV...")
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
    filtered = []
    for ch in raw_channels:
        name = ch['name']
        if any(kw in name.lower() for kw in EXCLUDE_KEYWORDS):
            continue
        filtered.append(ch)
    logger.info(f"После исключения ключевых слов: {len(filtered)}")

    # ---------- ОПТИМИЗАЦИЯ: дедупликация ДО проверки ----------
    # Группируем по нормализованному названию
    groups = defaultdict(list)
    for ch in filtered:
        norm = normalize_name(ch['name'])
        groups[norm].append(ch)

    # Выбираем лучшего кандидата из каждой группы (по приоритету)
    def sort_key(ch):
        source = extract_source_name(ch['source'])
        pref_score = 0
        for i, pref in enumerate(PREFERRED):
            if pref in source:
                pref_score = len(PREFERRED) - i
                break
        return (pref_score, ch.get('duration', 0) or 0, ch['name'])

    candidates = [max(ch_list, key=sort_key) for ch_list in groups.values()]
    logger.info(f"Кандидатов после дедупликации: {len(candidates)}")

    # ---------- Валидация потоков (только для кандидатов) ----------
    validated_channels = []
    if CHECK_STREAM:
        logger.info("Проверка доступности кандидатов (сначала быстрая HTTP)...")
        # Сначала быстрая HTTP-проверка для отсева заведомо мёртвых
        sem_http = asyncio.Semaphore(CONCURRENT * 2)
        async def http_check_one(ch):
            async with sem_http:
                if await http_quick_check(ch['url'], timeout=5):
                    return ch, True
                else:
                    return ch, False
        http_tasks = [http_check_one(ch) for ch in candidates]
        http_results = await asyncio.gather(*http_tasks)
        http_ok = [ch for ch, ok in http_results if ok]
        logger.info(f"После HTTP-проверки осталось: {len(http_ok)}")

        # Затем глубокая проверка ffprobe (если включена)
        if http_ok:
            logger.info("Глубокая проверка через ffprobe...")
            sem = asyncio.Semaphore(CONCURRENT)
            async def deep_check_one(ch):
                async with sem:
                    valid, duration = await validate_stream_deep(ch['url'], MIN_DURATION)
                    ch['valid'] = valid
                    ch['duration'] = duration
                    return ch
            deep_tasks = [deep_check_one(ch) for ch in http_ok]
            deep_results = await asyncio.gather(*deep_tasks)
            validated_channels = [ch for ch in deep_results if ch['valid']]
            logger.info(f"После глубокой проверки: {len(validated_channels)}")
        else:
            validated_channels = []
    else:
        validated_channels = candidates

    final_channels = validated_channels

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
        'after_validation': len(validated_channels),
        'final_count': len(final_channels),
        'duration_seconds': (datetime.now() - start_time).total_seconds()
    }
    with open(STATS_FILE, 'w', encoding='utf-8') as f:
        json.dump(stats, f, indent=2, ensure_ascii=False)

    logger.info(f" Готово! Плейлист сохранён в {OUTPUT_FILE}")
    logger.info(f" Статистика: {stats}")

if __name__ == '__main__':
    asyncio.run(main())