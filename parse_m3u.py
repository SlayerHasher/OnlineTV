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
CONCURRENT = CONFIG.get('concurrent_requests', 50)
CHECK_STREAM = CONFIG.get('check_stream', True)
PREFERRED = CONFIG.get('prefered_sources', [])
EXCLUDE_KEYWORDS = CONFIG.get('exclude_keywords', [])

# Ключевые слова для определения заглушек и ошибок в потоке
STREAM_ERROR_KEYWORDS = [
    'wink не показывает',
    'не верный токен',
    'неверный токен',
    'invalid token',
    '403 forbidden',
    '404 not found',
    'stream offline',
    'вещание временно приостановлено',
    'технические работы',
    'no signal',
    'нет сигнала',
    'test pattern',
    'заглушка',
    'ошибка доступа',
    'access denied',
    'unauthorized',
    'forbidden',
    'not found',
    'service unavailable'
]

STREAM_ERROR_EXCLUDE = CONFIG.get('stream_error_exclude', STREAM_ERROR_KEYWORDS)
KEEP_GROUP = CONFIG.get('keep_group_title', True)
SORT_BY_GROUP = CONFIG.get('sort_by_group', True)
RUSSIAN_ONLY = CONFIG.get('russian_only', True)
STANDARD_CATEGORIES = CONFIG.get('standard_categories', True)

# Стандартные категории для группировки
CATEGORY_MAPPING = {
    'новости': 'Новости',
    'news': 'Новости',
    'информ': 'Новости',
    'вест': 'Новости',
    '24': 'Новости',
    'мир': 'Новости',
    'звезда': 'Новости',
    'твц': 'Новости',
    'общественное': 'Новости',
    
    'фильм': 'Фильмы',
    'кино': 'Фильмы',
    'cinema': 'Фильмы',
    'movie': 'Фильмы',
    'сериал': 'Сериалы',
    'serial': 'Сериалы',
    'драма': 'Сериалы',
    
    'спорт': 'Спорт',
    'sport': 'Спорт',
    'match': 'Спорт',
    'eurospor': 'Спорт',
    'футбол': 'Спорт',
    'хоккей': 'Спорт',
    
    'детск': 'Детские',
    'детский': 'Детские',
    'kids': 'Детские',
    'children': 'Детские',
    'cartoon': 'Детские',
    'мульт': 'Детские',
    'аним': 'Детские',
    'disney': 'Детские',
    'nickelodeon': 'Детские',
    
    'музык': 'Музыка',
    'music': 'Музыка',
    'mtv': 'Музыка',
    'ru.tv': 'Музыка',
    'бриз': 'Музыка',
    'жара': 'Музыка',
    'шоу-тв': 'Музыка',
    
    'познават': 'Познавательные',
    'документ': 'Познавательные',
    'docu': 'Познавательные',
    'наука': 'Познавательные',
    'science': 'Познавательные',
    'history': 'Познавательные',
    'national geographic': 'Познавательные',
    'animal': 'Познавательные',
    'фауна': 'Познавательные',
    'природа': 'Познавательные',
    'travel': 'Познавательные',
    'туризм': 'Познавательные',
    'охота': 'Познавательные',
    'рыбалка': 'Познавательные',
    'кухня': 'Познавательные',
    'еда': 'Познавательные',
    'здоров': 'Познавательные',
    'мото': 'Познавательные',
    'авто': 'Познавательные',
    
    'развлеч': 'Развлекательные',
    'entertain': 'Развлекательные',
    'comedy': 'Развлекательные',
    'юмор': 'Развлекательные',
    'тнт': 'Развлекательные',
    'стс': 'Развлекательные',
    'рен-тв': 'Развлекательные',
    'пятница': 'Развлекательные',
    'tv3': 'Развлекательные',
    'домашний': 'Развлекательные',
    'перец': 'Развлекательные',
    'че': 'Развлекательные',
    'из': 'Развлекательные',
    
    'премиум': 'Премиум',
    'premium': 'Премиум',
    'vip': 'Премиум',
    'gold': 'Премиум',
    'platinum': 'Премиум',
    
    'религия': 'Религиозные',
    'religion': 'Религиозные',
    'православ': 'Религиозные',
    'церковь': 'Религиозные',
    
    'время': 'Детям и мамам',
    'мама': 'Детям и мамам',
    'mama': 'Детям и мамам',
    'мамонтенок': 'Детям и мамам',
    'gulli': 'Детям и мамам',
    
    'радио': 'Радиоканалы',
    'radio': 'Радиоканалы',
    'audio': 'Радиоканалы',
}

# Список крупных городов России и СНГ для фильтрации каналов с веб-камерами
CAMERA_CITY_KEYWORDS = [
    # Россия - города-миллионники и крупные города
    'москва', 'санкт-петербург', 'спб', 'питер', 'новосибирск', 'екатеринбург',
    'казань', 'нижний новгород', 'челябинск', 'самара', 'омск', 'ростов',
    'ростов-на-дону', 'уфа', 'красноярск', 'воронеж', 'пермь', 'волгоград',
    'краснодар', 'саратов', 'тюмень', 'тольятти', 'ижевск', 'барнаул',
    'ульяновск', 'иркутск', 'хабаровск', 'ярославль', 'владивосток', 'махачкала',
    'томск', 'оренбург', 'кемерово', 'новокузнецк', 'рязань', 'астрахань',
    'пенза', 'липецк', 'киров', 'тула', 'чебоксары', 'калининград', 'курск',
    'брянск', 'иваново', 'магнитогорск', 'тверь', 'ставрополь', 'севастополь',
    'симферополь', 'набережные челны', 'нижний тагил',
    'кострома', 'мурманск', 'архангельск', 'сургут', 'владикавказ', 'грозный',
    'петрозаводск', 'йошкар-ола', 'сыктывкар', 'саранск', 'элиста',
    # Крым и юг
    'ялта', 'севастополь', 'симферополь', 'евпатория', 'керчь', 'феодосия',
    'алупка', 'судак', 'бахчисарай', 'джанкой', 'красноперекопск',
    'анапа', 'геленджик', 'новороссийск', 'туапсе', 'адлер', ' сочи ',
    'лоо', 'дагомыс', 'лазаревское', 'сириус',
    # Курорты
    'минеральные воды', 'пятигорск', 'кисловодск', 'ессентуки', 'железноводск',
    'домбай', 'архыз', 'приэльбрусье', 'терскол',
    'алтай', 'горно-алтайск', 'белокуриха', 'чемал', 'телецкое',
    'байкал', 'иркутск', 'листвянка', 'слудянка', 'байкальск', 'северобайкальск',
    'камчатка', 'петропавловск-камчатский', 'вилючинск', 'елезово',
    'сахалин', 'южно-сахалинск', 'корсаков', 'холмск',
    # Другие известные туристические места
    'золотое кольцо', 'суздаль', 'владимир', 'сергиев посад', 'переславль',
    'углич', 'мышкин', 'рыбинск', 'романов', 'борисоглебск',
    'карелия', 'петрозаводск', 'сортавала', 'рускеала', 'кемь', 'беломорск',
    'кижи', 'валаам', 'кондопога',
    'кол', 'мурманская область', 'апатиты', 'кировск', 'мончегорск',
    'воркута', 'сале', 'новый уренгой', 'ноябрьск', 'надым',
    'мирный', 'ленск', 'удачный', 'айхал',
    'норильск', 'дудинка', 'талнах', 'кайеркан',
    'магадан', 'анадырь', 'провидения', 'певек', 'тикси',
]

# Русские слова для фильтрации
RUSSIAN_KEYWORDS = [
    'россия', 'русский', 'москва', 'спб', 'питер', 'сибирь', 'урал',
    'волга', 'дон', 'кубань', 'камчатка', 'байкал', 'крым',
    'первый', 'второй', 'третий', 'четвертый', 'пятый',
    'матч', 'культура', 'обучение', 'доверие', 'мир', 'звезда',
    'твц', 'нтв', 'рен', 'тнт', 'стс', 'тв3', 'пятница', 'домашний',
    'карusel', 'карусель', 'мульт', ' Ani ', 'аниме',
    'плюс', 'минус', 'радость', 'солнце', 'луна',
    'вгтрк', 'газпром', 'супер', 'че', 'из', '360', 'subbot', 'воскрес',
    'кино', 'фильм', 'сериал', 'драма', 'комедия',
    'спорт', 'футбол', 'хоккей', 'боец', 'кхл', 'эфир',
    'муз', 'музыка', 'жара', 'bridge', 'ру', 'europa', 'europaplus',
    'погода', 'время', 'утро', 'вечер', 'день', 'ночь',
    'кухня', 'еда', 'продукты', 'магазин', 'shop', 'mall',
    'мужской', 'женский', 'мода', 'style', 'glamour',
    'техно', 'наука', '24 тех', 'космос', 'нску',
    'история', 'секрет', 'тайна', 'легенда',
    'живая', 'природа', 'animal', 'планета', 'земля', 'космос',
    'путешеств', 'туризм', 'отпуск', 'отдых', 'дача', 'дом', 'ремонт',
    'здоров', 'доктор', 'медицина', 'врач', 'больница',
    'авто', 'мотор', 'drive', 'speed', 'road', 'track',
    'охота', 'рыбалка', 'лес', 'поле', 'горы', 'море', 'река',
    'ребенок', 'дет', 'baby', 'kid', 'junior', 'teen', 'family',
    'мама', 'папа', 'семья', 'родной', 'родина', 'отечество',
    'новость', 'вест', 'информ', 'репортаж', 'событие', 'актуаль',
    'полит', 'экономика', 'финанс', 'бизнес', 'money', 'wealth',
    'общество', 'люди', 'личность', 'герой', 'слава', 'успех',
    'игра', 'викторин', 'квн', 'шоу', 'концерт', 'фестиваль',
    'спектакль', 'театр', 'опера', 'балет', 'симфония',
    'арт', 'галерея', 'музей', 'выставка', 'коллекция',
    'книга', 'литература', 'писатель', 'поэт', 'стих',
    'школа', 'учеба', 'знание', 'ум', 'разум', 'мысль',
    'вера', 'бог', 'храм', 'монастырь', 'святой', 'православ',
    'islam', 'muslim', 'мечеть', 'коран',
    'еврей', 'jewish', 'tora', 'синагога',
    'будд', 'dalai', 'лама', 'тибет',
    'инду', 'krishna', 'rama', 'yoga', 'meditation',
    'эзо', 'астро', 'таро', 'гороскоп', 'знак', 'зодиак',
    'магия', 'ведьма', 'колдун', 'экстрасенс', 'пара', 'нло', 'пришель',
    'сон', 'сновид', 'подсознан', 'фрейд', 'юн',
    'психолог', 'психо', 'терапия', 'тренинг', 'коуч',
    'стиль', 'мода', 'красота', 'косметика', 'парикмахер', 'салон',
    'фото', 'видео', 'камера', 'объектив', 'линза', 'пленка',
    'компьютер', 'софт', 'программа', 'приложение', 'гаджет',
    'интернет', 'сеть', 'онлайн', 'веб', 'сайт', 'блог', 'влог',
    'соцсеть', 'чат', 'мессенджер', 'email', 'почта',
    'новостной', 'информагент', 'пресс', 'журнал', 'газета',
    'реклама', 'промо', 'маркетинг', 'бренд', 'товар', 'услуга',
    'работа', 'вакансия', 'резюме', 'карьера', 'бизнес',
    'недвижимость', 'квартира', 'дом', 'офис', 'склад', 'земля',
    'авто', 'машина', 'vehicle', 'transport', 'logistic',
    'страхов', 'банк', 'кредит', 'ипотека', 'депозит', 'инвест',
    'юрист', 'адвокат', 'нотариус', 'суд', 'полиция', 'мвд', 'фсб',
    'армия', 'воен', 'солдат', 'офицер', 'генерал', 'флот', 'авиа',
    'мчс', 'пожар', 'спас', 'скорая', 'медик', 'больница', 'клиника',
    'аптека', 'лекарство', 'таблетка', 'витамин', 'бад',
    'спорт', 'физкультур', 'зарядка', 'гимнастика', 'атлет',
    'чемпион', 'победа', 'золото', 'серебро', 'бронза', 'медаль',
    'лига', 'турнир', 'кубок', 'плей-офф', 'финал', 'полуфинал',
    'тренер', 'игрок', 'команда', 'клуб', 'фederation', 'ассоциа',
    'стадион', 'арена', 'площадка', 'кор', 'ринг', 'дорожка',
    'мяч', 'шайба', 'ракетка', 'клюшка', 'лыжа', 'конек', 'ролики',
    'велосипед', 'мото', 'auto', 'ралли', 'гонка', 'трек', 'кольцо',
    'плавание', 'бассейн', 'вода', 'ныряние', 'серфинг', 'яхта',
    'горы', 'скалы', 'альпинизм', 'поход', 'кемпинг', 'палатка',
    'охота', 'рыбалка', 'лес', 'грибы', 'ягоды', 'орехи',
    'сад', 'огород', 'дача', 'урожай', 'семена', 'саженцы',
    'животные', 'птицы', 'рыбы', 'насекомые', 'звери', 'pets',
    'кошка', 'собака', 'хомяк', 'попугай', 'аквариум', 'террариум',
    'ферма', 'деревня', 'село', 'агро', 'трактор', 'комбайн',
    'кухня', 'повар', 'шеф', 'ресторан', 'кафе', 'бар', 'клуб',
    'еда', 'блюдо', 'рецепт', 'ингредиент', 'специя', 'соус',
    'выпечка', 'десерт', 'торт', 'пирог', 'печенье', 'пряник',
    'напиток', 'чай', 'кофе', 'сок', 'лимонад', 'коктейль', 'алкоголь',
    'вино', 'водка', 'коньяк', 'виски', 'пиво', 'шампанское',
    'праздник', 'новый год', 'рождество', 'пасха', 'день рождения',
    'свадьба', 'юбилей', 'годовщина', 'торжество', 'церемония',
    'подарок', 'цветы', 'открытка', 'поздравление', 'пожелание',
    'путешествие', 'туризм', 'отпуск', 'каникулы', 'выходные',
    'отель', 'гостиница', 'хостел', 'апартаменты', 'вилла',
    'экскурсия', 'гид', 'маршрут', 'достопримечательность',
    'пляж', 'море', 'океан', 'озеро', 'река', 'водопад', 'источник',
    'город', 'столица', 'центр', 'район', 'округ', 'область', 'край',
    'республика', 'автономия', 'округ', 'поселок', 'село', 'деревня',
    'улица', 'проспект', 'бульвар', 'площадь', 'переулок', 'тупик',
    'дом', 'корпус', 'строение', 'этаж', 'квартира', 'комната',
    'офис', 'кабинет', 'зал', 'аудитория', 'класс', 'лаборатория',
    'школа', 'лицей', 'гимназия', 'колледж', 'техникум', 'вуз',
    'университет', 'академия', 'институт', 'факультет', 'кафедра',
    'студент', 'ученик', 'школьник', 'дошкольник', 'малыш',
    'учитель', 'преподаватель', 'профессор', 'доцент', 'лектор',
    'директор', 'завуч', 'ректор', 'декан', 'председатель',
    'классный', 'куратор', 'воспитатель', 'няня', 'гувернер',
    'библиотека', 'читальный', 'абонемент', 'формуляр', 'каталог',
    'музей', 'галерея', 'выставка', 'экспозиция', 'экспонат',
    'театр', 'сцена', 'подмостки', 'занавес', 'актер', 'актриса',
    'режиссер', 'сценарист', 'оператор', 'художник', 'гример',
    'костюмер', 'осветитель', 'звуковик', 'монтажер', 'продюсер',
    'концерт', 'спектакль', 'премьера', 'гастроли', 'фестиваль',
    'кино', 'фильм', 'картина', 'лента', 'кинолента', 'кинофильм',
    'сериал', 'сезон', 'серия', 'эпизод', 'пилот', 'спин-офф',
    'документальный', 'документалка', 'non-fiction', 'реальность',
    'анимация', 'мультфильм', 'мультсериал', 'аниме', 'манга',
    'комедия', 'драма', 'трагедия', 'мелодрама', 'боевик', 'триллер',
    'ужасы', 'хоррор', 'фантастика', 'фэнтези', 'сказка', 'притча',
    'детектив', 'криминал', 'нуар', 'вестерн', 'исторический',
    'биография', 'автобиография', 'мемуары', 'дневник', 'письмо',
    'роман', 'повесть', 'рассказ', 'новелла', 'эссе', 'очерк',
    'стихи', 'поэма', 'баллада', 'сонет', 'эпиграмма', 'частушка',
    'песня', 'романс', 'ария', 'дуэт', 'трио', 'квартет', 'хор',
    'опера', 'оперетта', 'мюзикл', 'рок-опера', 'кантата', 'оратория',
    'симфония', 'концерт', 'соната', 'сюита', 'фуга', 'прелюдия',
    'вальс', 'танго', 'фокстрот', 'самба', 'румба', 'ча-ча-ча',
    'джаз', 'блюз', 'рок', 'поп', 'рэп', 'хип-хоп', 'регги', 'кантри',
    'фолк', 'этно', 'народная', 'классика', 'академическая',
    'инструментал', 'вокал', 'хор', 'ансамбль', 'оркестр', 'группа',
    'солист', 'дирижер', 'композитор', 'поэт', 'певец', 'певица',
    'музыкант', 'инструмент', 'гитара', 'скрипка', 'фортепиано',
    'баян', 'аккордеон', 'балалайка', 'домра', 'гусли', 'жалейка',
    'ударные', 'барабан', 'тарелки', 'ксилофон', 'колокольчики',
    'духовые', 'труба', 'тромбон', 'саксофон', 'кларнет', 'флейта',
    'струнные', 'контрабас', 'виолончель', 'альт', 'арфа', 'лютня'
]

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

def is_russian_channel(channel: Dict) -> bool:
    """Проверяет, является ли канал русским по названию и группе."""
    name_lower = channel['name'].lower()
    group_lower = channel['attrs'].get('group-title', '').lower()
    text_to_check = f"{name_lower} {group_lower}"
    
    # Проверяем наличие русских ключевых слов
    for keyword in RUSSIAN_KEYWORDS:
        if keyword in text_to_check:
            return True
    
    # Проверяем наличие кириллицы в названии или группе
    if re.search(r'[а-яА-ЯёЁ]', text_to_check):
        return True
    
    return False

def get_standard_category(channel: Dict) -> str:
    """Определяет стандартную категорию для канала на основе названия и группы."""
    name_lower = channel['name'].lower()
    group_lower = channel['attrs'].get('group-title', '').lower()
    text_to_check = f"{name_lower} {group_lower}"
    
    # Проверяем маппинг категорий
    for keyword, category in CATEGORY_MAPPING.items():
        if keyword in text_to_check:
            return category
    
    # Категория по умолчанию
    return 'Другие'

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

# Слова-заглушки и ошибки в потоках
STREAM_ERROR_KEYWORDS = [
    'wink не показывает',
    'не верный токен',
    'неверный токен',
    'invalid token',
    'access denied',
    'forbidden',
    '403',
    '404',
    'not found',
    'stream offline',
    'канал временно недоступен',
    'технические работы',
    'no signal',
    'нет сигнала',
    'заглушка',
    'placeholder',
    'test card',
    'please subscribe',
    'подпишитесь',
    'ошибка доступа',
    'error access',
    'unauthorized',
    'authentication required',
    'требуется авторизация',
    'вещание приостановлено',
    'broadcast suspended',
    'content unavailable',
    'контент недоступен',
    # Камеры и веб-камеры (трансляции с улиц, площадей и т.д.)
    'веб-камера',
    'вебкамера',
    'web camera',
    'webcam',
    'камера онлайн',
    'камера города',
    'улица камера',
    'площадь камера',
    'камера улица',
    'камера площадь',
    'прямой эфир камера',
    'live camera',
    'city camera',
    'street camera',
    'traffic camera',
    'камера движения',
    'мониторинг камеры',
    'наблюдение камера',
    'cctv',
    'видеонаблюдение',
    'камера видеонаблюдения',
    'онлайн камера',
    'камера realtime',
    'панорама камера',
    'камера панорама',
]

# ---------- Быстрая HTTP-проверка ----------
async def http_check(url: str, timeout: int = 5) -> bool:
    """Проверяет доступность URL и отсутствие заглушек/ошибок."""
    try:
        async with aiohttp.ClientSession() as session:
            # Сначала HEAD-запрос для быстрой проверки статуса
            async with session.head(url, timeout=timeout) as resp:
                if resp.status >= 400:
                    return False
                
                # Проверяем Content-Type - должен быть видео или поток
                content_type = resp.headers.get('Content-Type', '').lower()
                # Если это текст или HTML - скорее всего это ошибка
                if any(ct in content_type for ct in ['text/html', 'text/plain', 'application/json']):
                    # Но не отвергаем сразу - некоторые легитимные потоки могут иметь такой тип
                    # Нужно проверить содержимое
                    pass
            
            # GET-запрос с ограничением по размеру для проверки содержимого
            headers = {'Range': 'bytes=0-2048'}  # Первые 2KB
            async with session.get(url, headers=headers, timeout=timeout) as resp:
                if resp.status >= 400:
                    return False
                
                content_type = resp.headers.get('Content-Type', '').lower()
                
                # Если это явно HTML страница с ошибкой
                if 'text/html' in content_type:
                    text = await resp.text(errors='ignore')
                    text_lower = text.lower()
                    
                    # Проверяем на наличие ошибок в HTML
                    error_patterns = ['error', 'ошибка', 'denied', 'forbidden', 'unauthorized', 
                                     'token', '403', '404', 'not found', 'access']
                    if any(pattern in text_lower for pattern in error_patterns):
                        return False
                
                # Проверяем бинарные данные на текстовые заглушки
                # Некоторые провайдеры возвращают текст даже для видеопотоков
                try:
                    chunk = await resp.read()
                    # Пробуем декодировать как текст для проверки на заглушки
                    try:
                        text_sample = chunk.decode('utf-8', errors='ignore').lower()
                    except:
                        text_sample = chunk.decode('latin-1', errors='ignore').lower()
                    
                    # Проверяем на наличие слов-заглушек из конфигурируемого списка
                    for keyword in STREAM_ERROR_EXCLUDE:
                        if keyword.lower() in text_sample:
                            logger.debug(f"Найдена заглушка '{keyword}' в {url}")
                            return False
                            
                except Exception:
                    # Если не удалось прочитать - считаем поток валидным
                    pass
                
                return True
                
    except asyncio.TimeoutError:
        return False
    except aiohttp.ClientError:
        return False
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

    # Фильтрация по русским каналам (если включено)
    if RUSSIAN_ONLY:
        filtered = [ch for ch in raw_channels if is_russian_channel(ch)]
        logger.info(f"После фильтрации русских каналов: {len(filtered)}")
    else:
        filtered = raw_channels

    # Исключаем по ключевым словам
    filtered = [ch for ch in filtered if not any(kw in ch['name'].lower() for kw in EXCLUDE_KEYWORDS)]
    logger.info(f"После исключения ключевых слов: {len(filtered)}")

    # Исключаем каналы с веб-камерами городов (проверяем название и группу)
    def is_camera_channel(channel: Dict) -> bool:
        """Проверяет, является ли канал трансляцией с веб-камеры города."""
        name_lower = channel['name'].lower()
        group_lower = channel['attrs'].get('group-title', '').lower()
        text_to_check = f"{name_lower} {group_lower}"
        
        # Проверяем наличие ключевых слов камер
        camera_keywords = ['веб-камера', 'вебкамера', 'webcam', 'web camera', 'камера онлайн', 
                          'камера города', 'онлайн камера', 'live camera', 'city camera',
                          'улица камера', 'площадь камера', 'камера улица', 'камера площадь']
        if any(kw in text_to_check for kw in camera_keywords):
            # Если есть слово "камера", проверяем также на наличие названия города
            for city in CAMERA_CITY_KEYWORDS:
                if city in text_to_check:
                    return True
            # Или если просто явная камера без уточнения - тоже отфильтровываем
            if any(kw in text_to_check for kw in ['веб-камера', 'вебкамера', 'webcam', 'web camera']):
                return True
        
        return False
    
    filtered = [ch for ch in filtered if not is_camera_channel(ch)]
    logger.info(f"После исключения веб-камер городов: {len(filtered)}")

    # Дедупликация: группировка по нормализованному названию + URL для точного совпадения
    groups = defaultdict(list)
    for ch in filtered:
        key = normalize_name(ch['name'])
        groups[key].append(ch)

    # Выбираем лучшего кандидата из каждой группы
    def sort_key(ch):
        source = extract_source_name(ch['source'])
        pref_score = 0
        for i, pref in enumerate(PREFERRED):
            if pref in source:
                pref_score = len(PREFERRED) - i
                break
        has_tvg_id = 1 if ch['attrs'].get('tvg-id') else 0
        has_group = 1 if ch['attrs'].get('group-title') else 0
        return (pref_score, has_tvg_id, has_group, ch['name'])

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

    # Применяем стандартные категории (если включено)
    if STANDARD_CATEGORIES:
        for ch in final_channels:
            standard_category = get_standard_category(ch)
            # Заменяем старую группу на новую стандартную категорию
            ch['attrs']['group-title'] = standard_category

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