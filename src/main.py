# ============================================================
# =  CONFIG & LOGGING
# ============================================================
from __future__ import annotations
import re
import glob
import json
import gzip
import requests
import logging
import os
import shutil
import heapq
from collections import defaultdict
from pathlib import Path
from typing import Dict, List
from datetime import datetime
from tqdm import tqdm

# --- Загрузка конфигурации ---
def load_config():
    config_path = Path(__file__).parent / "config.json"
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)

config = load_config()

# --- Настройка логирования ---
LOG_DIR = Path(config["paths"]["log_dir"])
LOG_DIR.mkdir(exist_ok=True)
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_file = LOG_DIR / f"trace-vrs-{timestamp}.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# --- Конфигурация ClickHouse ---
CLICKHOUSE_URL = config["clickhouse"]["url"]
CLICKHOUSE_USER = config["clickhouse"]["user"]
CLICKHOUSE_PASSWORD = config["clickhouse"]["password"]
CLICKHOUSE_DATABASE = config["clickhouse"]["database"]

# --- Параметры обработки ---
BATCH_SIZE = config["processing"]["batch_size"]
TIMEOUT = config["processing"]["timeout"]
DATASET_NAME = config["processing"]["dataset_name"]

# --- Ограничения фильтрации операций ---
MIN_OPERATION_DURATION_MS = config["filtering"]["min_operation_duration_ms"]
MIN_OPERATION_EVENTS = config["filtering"]["min_operation_events"]


# Reuse HTTP connection
SESSION = requests.Session()

# --- Таблицы ClickHouse ---
TABLE_OPERATIONS = "operations"
TABLE_EVENTS = "events"
TABLE_EVENT_STATS = "event_stats"

# --- Компиляция регулярных выражений ---
EVENT_START_RE = re.compile(r"^(\d{2}):(\d{2})\.(\d{6})-(\d+),([A-Za-z]+),")
PROPS_PATTERN = re.compile(r"\b(t:clientID|SessionID|Usr)=([^,\r\n]+)")
CONTEXT_PATTERN = re.compile(r',Context=(?:"([^"]*)"|\'([^\']*)\'|([^,\r\n]+))', re.DOTALL)


# ============================================================
# =  PARSING UTILITIES
# ============================================================

def get_file_line_count(file_path: str) -> int:
    """Быстро подсчитываем количество строк в файле"""
    try:
        with open(file_path, 'rb') as f:
            lines = 0
            buf_size = 1024 * 1024
            read_f = f.raw.read

            buf = read_f(buf_size)
            while buf:
                lines += buf.count(b'\n')
                buf = read_f(buf_size)
            
            return lines
    except Exception:
        return 0

def parse_event_start(line: str) -> dict | None:
    if m := EVENT_START_RE.match(line):
        return {
            "minute": int(m.group(1)),
            "second": int(m.group(2)),
            "microsecond": int(m.group(3)),
            "duration": int(m.group(4)),
            "event_name": m.group(5)
        }
    return None

def extract_props(event: str) -> dict:
    props = {}
    
    # Первый паттерн
    for match in PROPS_PATTERN.finditer(event):
        key, val = match.groups()
        props[key] = val.strip().strip("'\"")
    
    # Второй паттерн - Context (аналогично оригинальной extract_context)
    if match := CONTEXT_PATTERN.search(event):
        for val in match.groups():
            if val:
                props['Context'] = val.strip()
                break 

    return props

def update_op_props(op: dict, props: dict) -> None:
    """Обновляет session в active_ops, если они ещё не заданы."""
    if not op.get("session"):
        session_id = props.get("SessionID")
        # Проверяем что SessionID существует и состоит только из цифр
        if session_id and session_id.isdigit():
            op["session"] = session_id

def extract_first_field(events: list, key: str) -> str | None:
    for event in events:
        value = event.get("props", {}).get(key)
        if value:  # фильтруем None и пустые строки
            return value
    return None

_FILENAME_TIME_CACHE: dict[str, tuple[int, int, int, int]] = {}

def _get_time_parts_from_filename(filename: str) -> tuple[int, int, int, int]:
    cached = _FILENAME_TIME_CACHE.get(filename)
    if cached is not None:
        return cached
    # filename уже без пути (используем .name выше); берём без расширения, если оно есть
    dot = filename.rfind('.')
    base = filename[:dot] if dot != -1 else filename
    # YYMMDDHH...
    year = 2000 + int(base[0:2])
    month = int(base[2:4])
    day = int(base[4:6])
    hour = int(base[6:8])
    res = (year, month, day, hour)
    _FILENAME_TIME_CACHE[filename] = res
    return res

def build_ts(filename: str, meta: dict) -> datetime:
    year, month, day, hour = _get_time_parts_from_filename(filename)
    return datetime(year, month, day, hour, meta["minute"], meta["second"], meta["microsecond"])

def to_int_safe(value: str | int | None, default: int = 0) -> int:
    try:
        if value is None:
            return default
        if isinstance(value, int):
            return value
        return int(value)
    except (ValueError, TypeError):
        return default


# ============================================================
# =  CLICKHOUSE UTILITIES
# ============================================================

def insert_batch(table: str, rows: list[dict]) -> None:
    if not rows:
        return

    # <<< добавляем dataset в каждую строку перед сериализацией
    for r in rows:
        r["dataset"] = DATASET_NAME

    body_sql = "INSERT INTO {} FORMAT JSONEachRow\n{}".format(
        f"{CLICKHOUSE_DATABASE}.{table}",   # <<< пишем с префиксом БД
        "\n".join(json.dumps(r, ensure_ascii=False) for r in rows)
    ).encode("utf-8")
    gz = gzip.compress(body_sql)

    try:
        resp = SESSION.post(
            f"{CLICKHOUSE_URL}/",
            params={
                "user": CLICKHOUSE_USER, 
                "password": CLICKHOUSE_PASSWORD, 
                "database": CLICKHOUSE_DATABASE,
                "input_format_parallel_parsing": 0,
                "enable_http_compression": 1
            },
            data=gz,
            headers={
                "Content-Type": "text/plain; charset=utf-8",
                "Content-Encoding": "gzip",
                "Accept": "text/plain",
                # keep-alive по умолчанию
            },
            timeout=(5, TIMEOUT),
            proxies={}
        )
        if resp.status_code != 200:
            print(f"[ERROR] Insert into {table} failed: HTTP {resp.status_code}. Body: {resp.text[:800]} (rows={len(rows)})")
    except requests.RequestException as e:
        print(f"[ERROR] Insert failed into {table}: {e} (rows={len(rows)})")

# <<< быстрая очистка текущего набора данных
def clear_dataset(dataset: str) -> None:
    for table in (TABLE_EVENTS, TABLE_OPERATIONS, TABLE_EVENT_STATS):
        q = f"ALTER TABLE {CLICKHOUSE_DATABASE}.{table} DELETE WHERE dataset = '{dataset}'"
        try:
            resp = SESSION.post(
                f"{CLICKHOUSE_URL}/",
                params={"user": CLICKHOUSE_USER, "password": CLICKHOUSE_PASSWORD, "database": CLICKHOUSE_DATABASE, "query": q},
                timeout=TIMEOUT,
                proxies={}
            )
            if resp.status_code == 200:
                print(f"[INFO] Очищен набор данных '{dataset}' в таблице {table}")
            else:
                # Если таблицы нет (например, context_tree_rows) — просто сообщим
                print(f"[WARN] Clear dataset in {table}: HTTP {resp.status_code}, {resp.text[:200]}")
        except requests.RequestException as e:
            print(f"[WARN] Clear dataset in {table} failed: {e}")


# ============================================================
# =  OPERATIONS & EVENT STATS
# ============================================================

def calculate_event_stats(op: dict) -> dict:
    """
    Вычисляет статистику длительности событий по именам для операции.
    Исключает VRSREQUEST и VRSRESPONSE из статистики.
    Возвращает словарь с суммарной длительностью, процентом и количеством для каждого типа события.
    """
    event_stats = {}  # {event_name: {"total_duration_us": int, "count": int}}
    
    for event_data in op["events"]:
        meta = event_data["meta"]
        event_name = meta["event_name"]
        
        # Исключаем VRSREQUEST и VRSRESPONSE из статистики
        if event_name in ("VRSREQUEST", "VRSRESPONSE"):
            continue
            
        duration_us = int(meta["duration"])
        
        if event_name in event_stats:
            event_stats[event_name]["total_duration_us"] += duration_us
            event_stats[event_name]["count"] += 1
        else:
            event_stats[event_name] = {
                "total_duration_us": duration_us,
                "count": 1
            }
    
    # Вычисляем общую длительность всех событий
    total_events_duration = sum(stats["total_duration_us"] for stats in event_stats.values())
    
    # Добавляем процент к каждому типу события
    for event_name, stats in event_stats.items():
        duration_us = stats["total_duration_us"]
        percentage = round(duration_us * 100.0 / total_events_duration, 2) if total_events_duration > 0 else 0.0
        stats["percentage"] = percentage
    
    return event_stats


# ============================================================
# =  MULTI-CATALOG HANDLING
# ============================================================

def discover_rphost_groups(input_dir: str) -> dict[str, list[str]]:
    """
    Рекурсивно ищет каталоги rphost_* в input_dir.
    Если input_dir уже является каталогом rphost_*, возвращает только его.
    """
    groups = defaultdict(list)
    root = Path(input_dir).resolve()

    # --- 1. Если сам input_dir уже rphost_* ---
    if root.name.startswith("rphost_") and root.is_dir():
        groups[root.name].append(str(root))
        return dict(groups)

    # --- 2. Иначе ищем рекурсивно внутри ---
    for rphost_dir in root.rglob("rphost_*"):  # рекурсивный обход на любой глубине
        if rphost_dir.is_dir():
            groups[rphost_dir.name].append(str(rphost_dir))

    return dict(groups)

def group_files_by_hour(files: list[str]) -> dict[str, list[str]]:
    """
    Группирует список файлов по часу.
    Ключ часа – первые 8 символов имени файла: YYMMDDHH (как в build_ts).
    """
    grouped = defaultdict(list)
    for fp in files:
        base = Path(fp).stem
        if len(base) >= 8:
            hour_key = base[:8]
            grouped[hour_key].append(fp)
    return grouped

def iter_events(file_path: str):
    """
    Итератор событий из одного файла.
    Возвращает кортеж (ts: datetime, event_text: str), где event_text – событие целиком.
    """
    filename = Path(file_path).name
    buffer: list[str] = []
    event_meta: dict | None = None

    try:
        with open(file_path, "r", encoding="utf-8-sig", errors="ignore") as f:
            for line in f:
                ev_start = parse_event_start(line)
                if ev_start:
                    # закончим предыдущее событие
                    if buffer and event_meta:
                        ts = build_ts(filename, event_meta)
                        yield ts, "".join(buffer)
                        buffer.clear()
                    # начнем новое
                    event_meta = ev_start
                    buffer.append(line)
                else:
                    buffer.append(line)

            # хвост
            if buffer and event_meta:
                ts = build_ts(filename, event_meta)
                yield ts, "".join(buffer)

    except Exception as e:
        print(f"[WARN] Ошибка чтения {file_path}: {e}")

def merge_hour_events(hour_files: list[str], out_file: Path) -> int:
    """
    Объединяет события из списка файлов за один час в out_file по временной метке.
    Имя out_file должно быть вида <YYMMDDHH>.log (без суффиксов).
    Возвращает количество записанных событий.
    """
    # heap элементов: (ts, idx, text, gen)
    # idx нужен для устойчивости сравнения при равных ts
    heap: list[tuple[datetime, int, str, object]] = []
    idx_counter = 0

    # подготовим генераторы и закинем по первому событию в кучу
    gens = []
    for fp in hour_files:
        gen = iter_events(fp)
        try:
            ts, text = next(gen)
            heapq.heappush(heap, (ts, idx_counter, text, gen))
            idx_counter += 1
            gens.append(gen)
        except StopIteration:
            continue

    out_file.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with open(out_file, "w", encoding="utf-8-sig") as out:
        while heap:
            ts, _, text, gen = heapq.heappop(heap)
            out.write(text)
            count += 1
            try:
                ts2, text2 = next(gen)
                heapq.heappush(heap, (ts2, idx_counter, text2, gen))
                idx_counter += 1
            except StopIteration:
                pass

    return count

def build_temp_for_multi(input_dir: str, temp_dir: str) -> None:
    temp_root = Path(temp_dir)
    if temp_root.exists():
        shutil.rmtree(temp_root)
    temp_root.mkdir(parents=True, exist_ok=True)

    rphost_groups = discover_rphost_groups(input_dir)
    print(f"[INFO] Найдено процессов: {len(rphost_groups)} – {', '.join(sorted(rphost_groups.keys()))}")

    for proc_name, rphost_dirs in sorted(rphost_groups.items()):
        print(f"[INFO] Процесс {proc_name}: каталогов {len(rphost_dirs)}")
        # соберем все .log этого процесса из всех типовых каталогов
        all_logs: list[str] = []
        for d in rphost_dirs:
            all_logs.extend(sorted(glob.glob(str(Path(d) / "*.log"))))

        if not all_logs:
            print(f"[WARN] У процесса {proc_name} нет логов – пропускаю")
            continue

        # сгруппируем по часу
        by_hour = group_files_by_hour(all_logs)
        out_proc_dir = temp_root / proc_name
        out_proc_dir.mkdir(parents=True, exist_ok=True)

        for hour_key, hour_files in sorted(by_hour.items()):
            out_file = out_proc_dir / f"{hour_key}.log"   # без суффиксов
            cnt = merge_hour_events(hour_files, out_file)
            print(f"[INFO]   {proc_name}/{out_file.name}: {cnt:,} событий из {len(hour_files)} файлов")

    print(f"[INFO] Временная структура собрана: {temp_root}")


# ============================================================
# =  MAIN LOG PARSER
# ============================================================

def process_logs(input_dir: str) -> None:
    files = sorted(glob.glob(str(Path(input_dir) / "**" / "rphost_*"  / "*.log"), recursive=True))
    logger.info(f"Начинаем обработку {len(files)} файлов")
    print(f"[INFO] Найдено {len(files)} лог-файлов для обработки")
    
    if not files:
        logger.warning("Не найдено лог-файлов для обработки")
        print("[WARN] Не найдено лог-файлов для обработки")
        return
    
    active_ops: Dict[str, Dict] = {}
    batch_ops, batch_events, batch_event_stats = [], [], []
    total_operations = 0

    # Прогресс для каждого файла отдельно
    for file_idx, file in enumerate(files, 1):
        file_name = Path(file).name
        file_size_mb = Path(file).stat().st_size / (1024 * 1024)
        
        print(f"\n[INFO] Файл {file_idx}/{len(files)}: {file_name} ({file_size_mb:.1f} МБ)")
        logger.info(f"Обрабатываем файл {file_idx}/{len(files)}: {file_name} ({file_size_mb:.1f} МБ)")
        
        # Подсчитываем строки для прогресс-бара
        print("[INFO] Подсчет строк в файле...")
        total_lines = get_file_line_count(file)
        
        if total_lines == 0:
            print("[WARN] Не удалось подсчитать строки или файл пуст")
            continue
            
        print(f"[INFO] Обработка {total_lines:,} строк...")
        
        # Прогресс-бар для строк текущего файла
        line_progress = tqdm(
            total=total_lines,
            desc=f"Файл {file_idx}/{len(files)}: {file_name[:30]}",
            unit="строк",
            unit_scale=True
        )
        with Path(file).open("r", encoding="utf-8-sig", errors="ignore") as f:
            buffer: List[str] = []
            event_meta: dict | None = None
            current_filename = Path(file).name

            def process_previous_buffer():
                nonlocal buffer, event_meta, total_operations
                if not buffer or event_meta is None:
                    return
                event_text = "".join(buffer)
                props = extract_props(event_text)
                client_id = props.get("t:clientID")
                session_id = props.get("SessionID")
                ts_event = build_ts(current_filename, event_meta)

                if client_id:
                    if event_meta["event_name"] == "VRSREQUEST":
                        active_ops[client_id] = {
                            "session": session_id,
                            "ts_vrsrequest": ts_event,
                            "events": [{"meta": event_meta, "props": props, "text": event_text, "ts": ts_event}],
                        }
                        update_op_props(active_ops[client_id], props)

                    elif event_meta["event_name"] == "VRSRESPONSE":
                        if client_id in active_ops:
                            op = active_ops[client_id]
                            update_op_props(op, props)
                            op["events"].append({"meta": event_meta, "props": props, "text": event_text, "ts": ts_event})
                            op["ts_vrsresponse"] = ts_event
                            delta = op["ts_vrsresponse"] - op["ts_vrsrequest"]
                            op["duration_us"] = delta.days * 86_400_000_000 + delta.seconds * 1_000_000 + delta.microseconds
                          
                            # Проверяем ограничения перед сохранением
                            duration_us = op.get("duration_us", 0)
                            events_count = len(op["events"])
                            
                            # Фильтруем по минимальному времени и количеству событий
                            min_duration_us = MIN_OPERATION_DURATION_MS * 1000  # конвертируем мс в мкс
                            if duration_us < min_duration_us or events_count < MIN_OPERATION_EVENTS:
                                # Пропускаем операцию, не соответствующую критериям
                                del active_ops[client_id]
                                return

                            if op.get("session"):
                                total_operations += 1
                               
                                batch_ops.append({
                                    "session_id": to_int_safe(op.get("session")),
                                    "client_id": to_int_safe(client_id),
                                    "user": extract_first_field(op["events"], "Usr"),
                                    "ts_vrsrequest_us": op["ts_vrsrequest"].isoformat(timespec="microseconds"),
                                    "ts_vrsresponse_us": op["ts_vrsresponse"].isoformat(timespec="microseconds"),
                                    "ts_vrsrequest": op["ts_vrsrequest"].replace(microsecond=0).isoformat(),
                                    "ts_vrsresponse": op["ts_vrsresponse"].replace(microsecond=0).isoformat(),
                                    "duration_us": to_int_safe(op.get("duration_us", 0)),
                                    "context": extract_first_field(op["events"], "Context"),
                                })

                                prev_ts = None
                                for ev_data in op["events"]:
                                    # Используем сохраненные временные метки и метаданные
                                    ts = ev_data["ts"]
                                    meta = ev_data["meta"]
                                    props = ev_data["props"]
                                    event_string= ev_data["text"]
                                    space = int((ts - prev_ts).total_seconds() * 1_000_000 - int(meta["duration"])) if prev_ts else 0
                                    prev_ts = ts

                                    row = {
                                        "session_id": to_int_safe(op.get("session")),
                                        "client_id": to_int_safe(client_id),
                                        "user": props.get("Usr"),
                                        "ts_vrsrequest_us": op["ts_vrsrequest"].isoformat(timespec="microseconds"),
                                        "ts_vrsresponse_us": op["ts_vrsresponse"].isoformat(timespec="microseconds"),
                                        "ts_event_us": ts.isoformat(timespec="microseconds"),
                                        "ts_event": ts.replace(microsecond=0).isoformat(),
                                        "event_name": meta["event_name"],
                                        "duration_us": int(meta["duration"]),
                                        "space_us": space,
                                        "event_string": event_string,
                                        "context": props.get("Context")
                                    }
                                    batch_events.append(row)

                                 # Вычисляем статистику событий
                                event_stats = calculate_event_stats(op)
                                for event_name, stats in event_stats.items():
                                    batch_event_stats.append({
                                        "session_id": to_int_safe(op.get("session")),
                                        "client_id": to_int_safe(client_id),
                                        "ts_vrsrequest_us": op["ts_vrsrequest"].isoformat(timespec="microseconds"),
                                        "ts_vrsresponse_us": op["ts_vrsresponse"].isoformat(timespec="microseconds"),
                                        "event_name": event_name,
                                        "total_duration_us": stats["total_duration_us"],
                                        "count": stats["count"],
                                        "percentage": stats["percentage"]
                                    })
                                
                            del active_ops[client_id]

                    else:
                        if client_id in active_ops:
                            op = active_ops[client_id]
                            update_op_props(op, props)
                            op["events"].append({"meta": event_meta, "props": props, "text": event_text, "ts": ts_event})

                if len(batch_events) >= BATCH_SIZE:
                    insert_batch(TABLE_OPERATIONS, batch_ops)
                    insert_batch(TABLE_EVENTS, batch_events)
                    insert_batch(TABLE_EVENT_STATS, batch_event_stats)
                    logger.info(f"Отправлен батч: {len(batch_events)} событий, {len(batch_ops)} операций")
                    batch_events.clear()
                    batch_ops.clear()
                    batch_event_stats.clear()

                buffer = []

            line_count = 0
            for line in f:
                line_count += 1
                # Обновляем прогресс каждые 10000 строк
                if line_count % 10000 == 0:
                    line_progress.update(10000)
                    line_progress.set_postfix({
                        "Операции": total_operations
                    })
                if ev_start := parse_event_start(line):
                    # обработать ранее накопленный буфер как завершённое событие
                    process_previous_buffer()
                    buffer = [line]
                    event_meta = ev_start
                else:
                    buffer.append(line)

            # Обработка последнего буфера файла
            if buffer:
                process_previous_buffer()
        
        line_progress.close()
        print(f"[INFO] Файл {file_name} обработан. Операций: {total_operations}")
    
    # Финальный батч
    if batch_events or batch_ops or batch_event_stats:
        insert_batch(TABLE_EVENTS, batch_events)
        insert_batch(TABLE_OPERATIONS, batch_ops)
        insert_batch(TABLE_EVENT_STATS, batch_event_stats)
        logger.info(f"Отправлен финальный батч: {len(batch_events)} событий, {len(batch_ops)} операций")

    completion_msg = f"Готово: обработано {total_operations} операций"
    logger.info(completion_msg)
    print(f"[INFO] {completion_msg}")

if __name__ == "__main__":
    logger.info(f"Запуск скрипта trace-vrs. Лог сохраняется в: {log_file}")
    print(f"[INFO] Лог сохраняется в: {log_file}")

    # очистим текущий dataset перед загрузкой
    clear_dataset(DATASET_NAME)

    INPUT_DIR = config["paths"]["input_dir"]
    TEMP_DIR = config["paths"].get("temp_dir", str(Path(INPUT_DIR).parent / "temp"))
    MODE = config["processing"].get("mode", "single")

    logger.info(f"Режим обработки: {MODE}")
    print(f"[INFO] Режим обработки: {MODE}")
    logger.info(f"Каталог логов: {INPUT_DIR}")
    print(f"[INFO] Каталог логов: {INPUT_DIR}")

    try:
        if MODE == "multi":
            # собрать временную структуру rphost_*/YYMMDDHH.log
            build_temp_for_multi(INPUT_DIR, TEMP_DIR)
            # запустить парсер single по temp_dir
            logger.info(f"Старт обработки temp каталога: {TEMP_DIR}")
            print(f"[INFO] Старт обработки temp каталога: {TEMP_DIR}")
            process_logs(TEMP_DIR)
            # почистить temp
            try:
                shutil.rmtree(TEMP_DIR, ignore_errors=True)
                logger.info(f"Temp удален: {TEMP_DIR}")
                print(f"[INFO] Temp удален: {TEMP_DIR}")
            except Exception as e:
                logger.warning(f"Не удалось удалить temp: {e}")
                print(f"[WARN] Не удалось удалить temp: {e}")
        else:
            # классический режим
            process_logs(INPUT_DIR)

        logger.info("Скрипт успешно завершен")
        print("[INFO] Скрипт успешно завершен")

    except Exception as e:
        logger.error(f"Ошибка выполнения скрипта: {e}", exc_info=True)
        print(f"[ERROR] Ошибка: {e}")
        raise

