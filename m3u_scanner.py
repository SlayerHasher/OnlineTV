import asyncio
import aiohttp
import json
import re
import logging
import os
from datetime import datetime
from typing import List, Set
from urllib.parse import urlparse

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Файлы
SOURCES_FILE = 'play.list'
OUTPUT_FILE = 'found_sources.txt'

# Поиск GitHub API
GITHUB_SEARCH_URL = "https://api.github.com/search/code"
GITHUB_TOKEN = os.environ.get('GITHUB_TOKEN', '')  # Опционально для увеличения лимитов

# Ключевые слова для поиска плейлистов
SEARCH_QUERIES = [
    'extension:m3u iptv',
    'extension:m3u8 iptv',
    'extension:m3u playlist',
    'extension:m3u8 playlist',
    'filename:iptv extension:m3u',
    'filename:tv extension:m3u',
    'filename:channels extension:m3u',
]

# Популярные репозитории с IPTV
KNOWN_REPOS = [
    'https://raw.githubusercontent.com/iptv-org/iptv/master/iptv.m3u',
    'https://raw.githubusercontent.com/iptv-org/iptv/master/streams/ru.m3u',
    'https://raw.githubusercontent.com/AlexFreeTV/FreeTV/main/channels.m3u',
    'https://raw.githubusercontent.com/blackhole-9/IPTV/main/iptv.m3u',
    'https://raw.githubusercontent.com/sapec/IPTV/main/playlist.m3u',
    'https://raw.githubusercontent.com/Playlist-Pleb/iptv/main/playlist.m3u',
    'https://raw.githubusercontent.com/ViXen0o/iptv/main/iptv.m3u',
    'https://raw.githubusercontent.com/Free-TV/IPTV/master/playlist.m3u',
    'https://raw.githubusercontent.com/LiveTVCollection/IPTV/main/iptv.m3u',
]

# Шаблоны URL для проверки
M3U_PATTERN = re.compile(r'https?://[^\s]+\.m3u8?', re.IGNORECASE)
HTTP_URL_PATTERN = re.compile(r'https?://[^\s]+', re.IGNORECASE)


async def check_m3u_validity(session: aiohttp.ClientSession, url: str) -> bool:
    """Проверяет, является ли URL валидным M3U плейлистом."""
    try:
        async with session.get(url, timeout=10) as resp:
            if resp.status != 200:
                return False
            
            content_type = resp.headers.get('Content-Type', '').lower()
            text = await resp.text()
            
            # Проверяем наличие сигнатуры M3U
            if '#EXTM3U' in text:
                return True
            
            # Если Content-Type указывает на текст и есть http ссылки
            if 'text/' in content_type and 'http' in text:
                lines = text.strip().split('\n')
                http_lines = [l for l in lines if l.startswith('http')]
                if len(http_lines) >= 3:  # Минимум 3 канала
                    return True
            
            return False
    except Exception as e:
        logger.debug(f"Ошибка проверки {url}: {e}")
        return False


async def search_github(session: aiohttp.ClientSession, query: str) -> Set[str]:
    """Ищет M3U файлы на GitHub через API."""
    found_urls = set()
    
    headers = {}
    if GITHUB_TOKEN:
        headers['Authorization'] = f'token {GITHUB_TOKEN}'
    
    params = {
        'q': query,
        'per_page': 100,
        'page': 1
    }
    
    try:
        for page in range(1, 4):  # Первые 3 страницы
            params['page'] = page
            async with session.get(GITHUB_SEARCH_URL, headers=headers, params=params, timeout=15) as resp:
                if resp.status == 403:
                    logger.warning("Превышен лимит GitHub API. Пропускаем...")
                    break
                
                if resp.status != 200:
                    logger.warning(f"GitHub API вернул статус {resp.status}")
                    break
                
                data = await resp.json()
                items = data.get('items', [])
                
                if not items:
                    break
                
                for item in items:
                    repo = item.get('repository', {})
                    file_path = item.get('path', '')
                    
                    if repo and file_path:
                        owner = repo.get('owner', {}).get('login', '')
                        repo_name = repo.get('name', '')
                        
                        # Формируем raw URL
                        raw_url = f"https://raw.githubusercontent.com/{owner}/{repo_name}/main/{file_path}"
                        found_urls.add(raw_url)
                        
                        # Пробуем альтернативные ветки
                        alt_urls = [
                            f"https://raw.githubusercontent.com/{owner}/{repo_name}/master/{file_path}",
                            f"https://raw.githubusercontent.com/{owner}/{repo_name}/develop/{file_path}",
                        ]
                        found_urls.update(alt_urls)
                
                # Если меньше 100 результатов - дальше искать нет смысла
                if len(items) < 100:
                    break
                    
    except Exception as e:
        logger.error(f"Ошибка при поиске GitHub '{query}': {e}")
    
    return found_urls


async def scan_html_for_m3u(session: aiohttp.ClientSession, url: str) -> Set[str]:
    """Сканирует HTML страницу на наличие ссылок на M3U файлы."""
    found_urls = set()
    
    try:
        async with session.get(url, timeout=15) as resp:
            if resp.status != 200:
                return found_urls
            
            html = await resp.text()
            
            # Ищем все ссылки на .m3u и .m3u8
            matches = M3U_PATTERN.findall(html)
            for match in matches:
                # Очищаем URL от лишних символов
                clean_url = match.split('"')[0].split("'")[0].strip()
                if clean_url.startswith('http'):
                    found_urls.add(clean_url)
            
            # Также ищем относительные ссылки
            rel_matches = re.findall(r'href=["\']([^"\']*\.m3u8?)["\']', html, re.IGNORECASE)
            base_url = f"{urlparse(url).scheme}://{urlparse(url).netloc}"
            
            for rel_url in rel_matches:
                if rel_url.startswith('http'):
                    found_urls.add(rel_url)
                elif rel_url.startswith('/'):
                    found_urls.add(f"{base_url}{rel_url}")
                else:
                    found_urls.add(f"{base_url}/{rel_url}")
                    
    except Exception as e:
        logger.debug(f"Ошибка сканирования {url}: {e}")
    
    return found_urls


async def check_known_repos(session: aiohttp.ClientSession) -> Set[str]:
    """Проверяет известные репозитории на доступность."""
    valid_urls = set()
    
    tasks = [check_m3u_validity(session, url) for url in KNOWN_REPOS]
    results = await asyncio.gather(*tasks)
    
    for url, is_valid in zip(KNOWN_REPOS, results):
        if is_valid:
            valid_urls.add(url)
            logger.info(f"Найден рабочий источник: {url}")
    
    return valid_urls


async def load_existing_sources() -> Set[str]:
    """Загружает существующие источники из файла."""
    existing = set()
    if os.path.exists(SOURCES_FILE):
        with open(SOURCES_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    existing.add(line)
    return existing


async def save_new_sources(new_sources: Set[str]):
    """Сохраняет новые источники в файл."""
    existing = await load_existing_sources()
    all_sources = existing.union(new_sources)
    
    with open(SOURCES_FILE, 'w', encoding='utf-8') as f:
        f.write("# OnlineTV - список источников\n")
        f.write("# Одна строка = один плейлист\n")
        f.write("# Пустые строки игнорируются\n\n")
        f.write("# === ИСТОЧНИКИ ===\n")
        for source in sorted(all_sources):
            f.write(f"{source}\n\n")
    
    # Сохраняем отчет о найденных
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.write(f"# Найдено источников: {len(new_sources)}\n")
        f.write(f"# Дата: {datetime.now().isoformat()}\n\n")
        for source in sorted(new_sources):
            f.write(f"{source}\n")
    
    logger.info(f"Новые источники сохранены в {OUTPUT_FILE}")
    logger.info(f"Обновленный список источников: {SOURCES_FILE}")


async def main():
    logger.info("Запуск сканера M3U плейлистов...")
    start_time = datetime.now()
    
    all_found_urls = set()
    
    async with aiohttp.ClientSession() as session:
        # 1. Проверяем известные репозитории
        logger.info("Проверка известных репозиториев...")
        known_valid = await check_known_repos(session)
        all_found_urls.update(known_valid)
        
        # 2. Поиск через GitHub API
        logger.info("Поиск на GitHub...")
        for query in SEARCH_QUERIES:
            logger.info(f"Поиск по запросу: {query}")
            found = await search_github(session, query)
            all_found_urls.update(found)
            await asyncio.sleep(1)  # Пауза между запросами
        
        # 3. Фильтрация и проверка найденных URL
        logger.info(f"Всего найдено потенциальных источников: {len(all_found_urls)}")
        logger.info("Проверка валидности найденных плейлистов...")
        
        # Проверяем только те, которых нет в существующих
        existing = await load_existing_sources()
        to_check = all_found_urls - existing
        
        valid_new_sources = set()
        semaphore = asyncio.Semaphore(20)  # Ограничиваем параллелизм
        
        async def check_with_semaphore(url):
            async with semaphore:
                if await check_m3u_validity(session, url):
                    logger.info(f"✓ Валидный: {url}")
                    return url
                return None
        
        tasks = [check_with_semaphore(url) for url in to_check]
        results = await asyncio.gather(*tasks)
        
        valid_new_sources = {url for url in results if url is not None}
        
        # 4. Сохранение результатов
        if valid_new_sources:
            logger.info(f"\n{'='*50}")
            logger.info(f"Найдено новых рабочих источников: {len(valid_new_sources)}")
            await save_new_sources(valid_new_sources)
        else:
            logger.info("\nНовых рабочих источников не найдено.")
    
    duration = (datetime.now() - start_time).total_seconds()
    logger.info(f"Сканирование завершено за {duration:.2f} сек.")


if __name__ == '__main__':
    asyncio.run(main())
