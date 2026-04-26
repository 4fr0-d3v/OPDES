# coding: utf-8

import json
import re
import shutil
import sys
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from requests.exceptions import (
    ChunkedEncodingError,
    ConnectionError,
    HTTPError,
    RequestException,
    Timeout,
)
from tqdm import tqdm
from urllib3.util.retry import Retry


CONFIG_DIR = Path.home() / ".opdes"
CONFIG_PATH = CONFIG_DIR / "config.json"

DEFAULT_CONFIG = {
    "url": "https://onepace.net/es/watch",
    "output_dir": "descargas_pixeldrain",
    "metadata_dir": "../one-pace-jellyfin-master/One Pace",
    "series_name": "OnePiece",
    "quality": "max",
    "log_level": "error",
}

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

API_HOSTS = [
    "pixeldrain.net",
    "pixeldrain.com",
]

CHUNK_SIZE = 1024 * 1024
MAX_REINTENTOS_DESCARGA = 8
MAX_REINTENTOS_JSON = 8

LOG_LEVELS = {
    "error": 0,
    "debug": 1,
}


def cargar_config():
    if not CONFIG_PATH.exists():
        guardar_config(DEFAULT_CONFIG)
        return DEFAULT_CONFIG.copy()

    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        guardar_config(DEFAULT_CONFIG)
        return DEFAULT_CONFIG.copy()

    config = DEFAULT_CONFIG.copy()
    config.update(data)
    return config


def guardar_config(config: dict):
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2, ensure_ascii=False)


def get_log_level(config: dict | None = None) -> str:
    if config is None:
        config = cargar_config()
    level = str(config.get("log_level", "error")).lower()
    return level if level in LOG_LEVELS else "error"


def should_log(level: str, config: dict | None = None) -> bool:
    current = get_log_level(config)
    return LOG_LEVELS.get(current, 0) >= LOG_LEVELS.get(level, 0)


def log_debug(msg: str, config: dict | None = None):
    if should_log("debug", config):
        print(f"[debug] {msg}")


def log_error(msg: str):
    print(f"[error] {msg}")


def mostrar_config():
    config = cargar_config()
    print(json.dumps(config, indent=2, ensure_ascii=False))
    print(f"\nArchivo de configuración: {CONFIG_PATH}")


def set_url(valor: str):
    config = cargar_config()
    config["url"] = valor
    guardar_config(config)
    print(f"[OK] url = {valor}")
    print(f"Guardado en: {CONFIG_PATH}")


def set_output(valor: str):
    config = cargar_config()
    config["output_dir"] = valor
    guardar_config(config)
    print(f"[OK] output_dir = {valor}")
    print(f"Guardado en: {CONFIG_PATH}")


def set_metadata(valor: str):
    config = cargar_config()
    config["metadata_dir"] = valor
    guardar_config(config)
    print(f"[OK] metadata_dir = {valor}")
    print(f"Guardado en: {CONFIG_PATH}")


def set_quality(valor: str):
    valor = valor.strip().lower()
    permitidos = {"max", "480p", "720p", "1080p"}

    if valor not in permitidos:
        print(f"[error] quality no válida: {valor}. Usa una de: {', '.join(sorted(permitidos))}")
        return

    config = cargar_config()
    config["quality"] = valor
    guardar_config(config)
    print(f"[OK] quality = {valor}")
    print(f"Guardado en: {CONFIG_PATH}")


def set_log_level(valor: str):
    valor = valor.strip().lower()
    permitidos = {"error", "debug"}

    if valor not in permitidos:
        print(f"[error] log_level no válido: {valor}. Usa una de: {', '.join(sorted(permitidos))}")
        return

    config = cargar_config()
    config["log_level"] = valor
    guardar_config(config)
    print(f"[OK] log_level = {valor}")
    print(f"Guardado en: {CONFIG_PATH}")


def validar_directorio_salida(output_dir: Path) -> bool:
    log_debug(f"Comprobando output_dir: {output_dir}")

    try:
        output_dir.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        log_error(f"No se pudo crear o acceder a output_dir: {output_dir} -> {e}")
        return False

    if not output_dir.exists():
        log_error(f"output_dir no existe tras intentar crearlo: {output_dir}")
        return False

    if not output_dir.is_dir():
        log_error(f"output_dir no es un directorio: {output_dir}")
        return False

    log_debug(f"output_dir OK: {output_dir}")
    return True


def validar_directorio_metadatos(metadata_dir: Path) -> bool:
    log_debug(f"Comprobando metadata_dir: {metadata_dir}")

    if not metadata_dir.exists():
        log_error(f"metadata_dir no existe, no se creará automáticamente: {metadata_dir}")
        return False

    if not metadata_dir.is_dir():
        log_error(f"metadata_dir no es un directorio: {metadata_dir}")
        return False

    seasons = sorted(metadata_dir.glob("Season *"))
    log_debug(f"Temporadas encontradas en metadata_dir: {len(seasons)}")

    if not seasons:
        log_error(f"No se encontraron carpetas 'Season *' dentro de: {metadata_dir}")
        return False

    seasons_con_nfo = 0
    for season_dir in seasons:
        season_nfo = season_dir / "season.nfo"
        log_debug(f"Revisando {season_dir.name} -> season.nfo existe={season_nfo.exists()}")
        if season_nfo.exists():
            seasons_con_nfo += 1

    if seasons_con_nfo == 0:
        log_error(f"No se encontró ningún season.nfo válido dentro de: {metadata_dir}")
        return False

    log_debug(f"metadata_dir OK: {metadata_dir} | temporadas con season.nfo: {seasons_con_nfo}")
    return True


def validar_configuracion(config: dict) -> bool:
    output_dir = Path(config.get("output_dir", DEFAULT_CONFIG["output_dir"])).expanduser()
    metadata_dir = Path(config.get("metadata_dir", DEFAULT_CONFIG["metadata_dir"])).expanduser()

    log_debug("Iniciando validación de configuración", config)
    log_debug(f"url configurada: {config.get('url')}", config)
    log_debug(f"series_name configurado: {config.get('series_name')}", config)
    log_debug(f"quality configurada: {config.get('quality')}", config)
    log_debug(f"log_level configurado: {config.get('log_level')}", config)

    ok_output = validar_directorio_salida(output_dir)
    ok_metadata = validar_directorio_metadatos(metadata_dir)

    if not ok_output or not ok_metadata:
        log_error("La configuración no es válida. Corrige las rutas antes de ejecutar.")
        return False

    log_debug("Validación de configuración completada correctamente", config)
    return True


def obtener_html(url: str) -> str:
    ultimo_error = None

    for intento in range(1, 6):
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            r.raise_for_status()
            return r.text
        except RequestException as e:
            ultimo_error = e
            if intento == 5:
                break
            espera = min(2 ** intento, 10)
            log_debug(f"Reintento HTML {intento}/5 para {url} -> {e}")
            time.sleep(espera)

    raise RuntimeError(f"No se pudo obtener el HTML de {url}: {ultimo_error}")


def extraer_temporadas_y_pixeldrain(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    temporadas = []

    for li in soup.find_all("li", id=True):
        season_id = li["id"]
        pixeldrain_links = []

        for a in li.find_all("a", href=True):
            href = a["href"].strip()
            if "pixeldrain.net" in href or "pixeldrain.com" in href:
                pixeldrain_links.append({
                    "texto": a.get_text(separator=" ", strip=True),
                    "url": href
                })

        temporadas.append({
            "id": season_id,
            "pixeldrain": pixeldrain_links
        })

    return temporadas


def slugify(value: str) -> str:
    value = value.strip().lower()
    value = re.sub(r"[^\w\-\.]+", "_", value)
    value = re.sub(r"_+", "_", value)
    return value.strip("_") or "sin_nombre"


def extraer_tipo_e_id(url: str):
    parsed = urlparse(url)
    path = parsed.path.strip("/")
    partes = path.split("/")

    if len(partes) >= 2:
        tipo, item_id = partes[0], partes[1]
        if tipo == "l":
            return "list", item_id
        if tipo == "u":
            return "file", item_id

    raise ValueError(f"URL de Pixeldrain no soportada: {url}")


def hosts_preferidos_desde_url(url: str):
    host = urlparse(url).netloc.lower()

    if "pixeldrain.net" in host:
        return ["pixeldrain.net", "pixeldrain.com"]
    if "pixeldrain.com" in host:
        return ["pixeldrain.com", "pixeldrain.net"]

    return API_HOSTS[:]


def crear_sesion():
    session = requests.Session()

    retry = Retry(
        total=0,
        connect=0,
        read=0,
        redirect=3,
    )

    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=1,
        pool_maxsize=1,
    )

    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({
        "User-Agent": "Mozilla/5.0",
        "Accept": "*/*",
    })
    return session


def pedir_json_resistente(path: str, url_original: str, max_intentos: int = MAX_REINTENTOS_JSON):
    ultimo_error = None
    hosts = hosts_preferidos_desde_url(url_original)

    for host in hosts:
        url = f"https://{host}/api{path}"

        for intento in range(1, max_intentos + 1):
            try:
                with requests.get(
                    url,
                    timeout=(10, 30),
                    headers={
                        "User-Agent": "Mozilla/5.0",
                        "Accept": "application/json",
                        "Connection": "close",
                    },
                ) as resp:
                    if resp.status_code == 404:
                        raise RuntimeError(f"No encontrado en Pixeldrain: {url}")

                    if resp.status_code == 403:
                        try:
                            detalle = resp.json()
                        except Exception:
                            detalle = {"message": "403 Forbidden"}
                        raise RuntimeError(
                            f"No se puede acceder a {url}: {detalle.get('message', '403 Forbidden')}"
                        )

                    resp.raise_for_status()
                    return resp.json()

            except (ConnectionError, Timeout, ChunkedEncodingError, OSError) as e:
                ultimo_error = e
                if intento == max_intentos:
                    break

                espera = min(2 ** intento, 20)
                log_debug(f"Reintento JSON {intento}/{max_intentos} para {url} -> {e}")
                time.sleep(espera)

            except HTTPError as e:
                ultimo_error = e
                break

    raise RuntimeError(f"Falló la petición JSON tras varios intentos: {ultimo_error}")


def descargar_archivo_reanudable(
    file_id: str,
    nombre_archivo: str,
    carpeta_destino: Path,
    session: requests.Session,
    url_original: str
):
    carpeta_destino.mkdir(parents=True, exist_ok=True)
    destino = carpeta_destino / nombre_archivo
    temp = destino.with_suffix(destino.suffix + ".part")

    if destino.exists():
        log_debug(f"Archivo ya existe, se omite: {destino}")
        return destino

    ultimo_error = None
    hosts = hosts_preferidos_desde_url(url_original)

    for host in hosts:
        url_descarga = f"https://{host}/api/file/{file_id}?download"

        for intento in range(1, MAX_REINTENTOS_DESCARGA + 1):
            descargado = temp.stat().st_size if temp.exists() else 0
            headers = {
                "Connection": "close",
                "User-Agent": "Mozilla/5.0",
            }

            if descargado > 0:
                headers["Range"] = f"bytes={descargado}-"

            try:
                with session.get(url_descarga, headers=headers, stream=True, timeout=(10, 120)) as resp:
                    if resp.status_code == 403:
                        try:
                            detalle = resp.json()
                        except Exception:
                            detalle = {"message": "403 Forbidden"}
                        raise RuntimeError(
                            f"No se puede descargar {file_id}: {detalle.get('message', '403 Forbidden')}"
                        )

                    if resp.status_code == 404:
                        raise RuntimeError(f"Archivo no encontrado: {file_id}")

                    if resp.status_code not in (200, 206):
                        raise RuntimeError(f"HTTP inesperado {resp.status_code} al descargar {file_id}")

                    modo = "ab" if resp.status_code == 206 and descargado > 0 else "wb"
                    if modo == "wb" and temp.exists():
                        temp.unlink()
                        descargado = 0

                    total = None
                    content_length = resp.headers.get("Content-Length")
                    if content_length and content_length.isdigit():
                        total_respuesta = int(content_length)
                        total = descargado + total_respuesta if resp.status_code == 206 else total_respuesta

                    with open(temp, modo) as f, tqdm(
                        total=total,
                        initial=descargado,
                        unit="B",
                        unit_scale=True,
                        unit_divisor=1024,
                        desc=nombre_archivo,
                        ascii=False,
                        dynamic_ncols=True,
                    ) as pbar:
                        for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                            if chunk:
                                f.write(chunk)
                                pbar.update(len(chunk))

                temp.rename(destino)
                return destino

            except (ConnectionError, Timeout, ChunkedEncodingError, OSError) as e:
                ultimo_error = e
                if intento == MAX_REINTENTOS_DESCARGA:
                    break

                espera = min(2 ** intento, 30)
                log_debug(f"Reintento descarga {intento}/{MAX_REINTENTOS_DESCARGA} para {file_id} -> {e}")
                time.sleep(espera)

            except RuntimeError:
                raise

    raise RuntimeError(
        f"Fallo descargando {file_id} tras {MAX_REINTENTOS_DESCARGA} intentos por dominio: {ultimo_error}"
    )


def extraer_calidad_desde_texto(texto: str) -> str | None:
    m = re.search(r"(480p|720p|1080p)", texto, re.IGNORECASE)
    return m.group(1).lower() if m else None


def ordenar_calidades(calidad: str) -> int:
    orden = {
        "480p": 480,
        "720p": 720,
        "1080p": 1080,
    }
    return orden.get(calidad.lower(), 0)


def agrupar_por_temporada(items: list[dict]) -> list[dict]:
    agrupado = {}

    for item in items:
        arc_id = item.get("id", "sin_id")
        bucket = agrupado.setdefault(arc_id, {
            "id": arc_id,
            "opciones": [],
        })

        for enlace in item.get("pixeldrain", []):
            calidad = extraer_calidad_desde_texto(enlace.get("texto", "")) or "desconocida"
            bucket["opciones"].append({
                "texto": enlace.get("texto", ""),
                "url": enlace.get("url", ""),
                "quality": calidad,
            })

    resultado = list(agrupado.values())

    for idx, arc in enumerate(resultado, start=1):
        arc["season_number"] = idx

    return resultado


def elegir_opcion_por_calidad(opciones: list[dict], quality_config: str) -> dict | None:
    if not opciones:
        return None

    opciones_validas = [x for x in opciones if x.get("quality") in {"480p", "720p", "1080p"}]
    if not opciones_validas:
        return opciones[0]

    opciones_ordenadas = sorted(opciones_validas, key=lambda x: ordenar_calidades(x["quality"]))

    if quality_config == "max":
        return opciones_ordenadas[-1]

    for op in opciones_ordenadas:
        if op["quality"] == quality_config:
            return op

    return opciones_ordenadas[-1]


def mostrar_opciones(opciones: list[dict]):
    print("\nTemporadas/arcos encontrados:\n")
    for op in opciones:
        print(f"{op['n']:>3}. {op['id']} | {op['texto']}")


def parsear_seleccion(texto: str, max_n: int) -> set[int]:
    seleccion = set()
    texto = texto.strip().lower()

    if texto in {"*", "all", "todo", "todos"}:
        return set(range(1, max_n + 1))

    partes = [p.strip() for p in texto.split(",") if p.strip()]
    for parte in partes:
        if "-" in parte:
            a, b = parte.split("-", 1)
            if a.isdigit() and b.isdigit():
                inicio, fin = int(a), int(b)
                if inicio > fin:
                    inicio, fin = fin, inicio
                for i in range(inicio, fin + 1):
                    if 1 <= i <= max_n:
                        seleccion.add(i)
        elif parte.isdigit():
            i = int(parte)
            if 1 <= i <= max_n:
                seleccion.add(i)

    return seleccion


def filtrar_opciones(opciones: list[dict]) -> list[dict]:
    mostrar_opciones(opciones)

    print("\nCómo seleccionar:")
    print("  - todos")
    print("  - índices: 1,3,8")
    print("  - rango: 5-12")
    print("  - mezcla: 1,3,5-8")
    print("  - filtro por arco: arc:romance-dawn")

    entrada = input("\nQué quieres descargar: ").strip()

    if not entrada:
        return opciones

    entrada_lower = entrada.lower()

    if entrada_lower in {"*", "all", "todo", "todos"}:
        return opciones

    tokens = entrada_lower.split()
    arc_filters = [t[4:] for t in tokens if t.startswith("arc:")]

    if arc_filters:
        resultado = opciones[:]
        for arc in arc_filters:
            resultado = [x for x in resultado if arc in x["id"].lower()]
        return resultado

    indices = parsear_seleccion(entrada, len(opciones))
    return [x for x in opciones if x["n"] in indices]


def pedir_carpeta_destino(config: dict) -> Path:
    default_dir = config.get("output_dir", DEFAULT_CONFIG["output_dir"])
    entrada = input(f"\nCarpeta de destino (vacío = {default_dir}): ").strip()
    if not entrada:
        return Path(default_dir).expanduser()
    return Path(entrada).expanduser()


def reconstruir_items_segun_calidad(agrupadas: list[dict], quality_config: str) -> list[dict]:
    resultado = []

    for arc in agrupadas:
        elegida = elegir_opcion_por_calidad(arc["opciones"], quality_config)
        if not elegida:
            continue

        resultado.append({
            "id": arc["id"],
            "season_number": arc["season_number"],
            "pixeldrain": [{
                "texto": elegida["texto"],
                "url": elegida["url"],
            }]
        })

    return resultado


def obtener_archivos_lista_pixeldrain(url: str) -> list[dict]:
    tipo, item_id = extraer_tipo_e_id(url)

    if tipo == "file":
        info = pedir_json_resistente(f"/file/{item_id}/info", url)
        return [info]

    data = pedir_json_resistente(f"/list/{item_id}", url)
    return data.get("files", [])


def leer_titulo_season_nfo(season_nfo: Path) -> str | None:
    try:
        root = ET.parse(season_nfo).getroot()
        title = root.findtext("title")
        return title.strip() if title else None
    except Exception:
        return None


def extraer_season_episode_de_nfo_name(nfo_name: str):
    m = re.search(r"S(\d+)E(\d+)", nfo_name, re.IGNORECASE)
    if not m:
        return None, None
    return int(m.group(1)), int(m.group(2))


def construir_indice_metadatos(metadata_root: Path):
    indice = {}

    if not metadata_root.exists():
        log_debug(f"metadata_root no existe para indexar: {metadata_root}")
        return indice

    for season_dir in sorted(metadata_root.glob("Season *")):
        season_nfo = season_dir / "season.nfo"
        if not season_nfo.exists():
            continue

        num_match = re.search(r"Season\s+(\d+)", season_dir.name, re.IGNORECASE)
        if not num_match:
            continue

        season_number = int(num_match.group(1))
        season_title = leer_titulo_season_nfo(season_nfo) or season_dir.name

        episodes = {}
        for ep_nfo in sorted(season_dir.glob("*.nfo")):
            if ep_nfo.name.lower() == "season.nfo":
                continue

            _, ep_num = extraer_season_episode_de_nfo_name(ep_nfo.name)
            if ep_num is not None:
                episodes[ep_num] = ep_nfo

        indice[season_number] = {
            "season_number": season_number,
            "season_title": season_title,
            "season_dir": season_dir,
            "season_nfo": season_nfo,
            "episodes": episodes,
        }

    return indice


def parsear_nombre_descargado(nombre_archivo: str):
    patron = re.compile(
        r"^\[One Pace\]\[[^\]]+\]\s+(.+?)\s+(\d{2})\s+\[[^\]]+\]\[[^\]]+\]\[[0-9A-Fa-f]{8}\](\.[^.]+)$"
    )
    m = patron.match(nombre_archivo)
    if not m:
        return None

    return {
        "arc_name": m.group(1).strip(),
        "episode_in_arc": int(m.group(2)),
        "ext": m.group(3),
    }


def asegurar_estructura_temporada(destino_base: Path, season_meta: dict, series_name: str) -> Path:
    season_number = season_meta["season_number"]
    carpeta_serie = destino_base / series_name
    carpeta_temporada = carpeta_serie / f"Season {season_number}"

    carpeta_temporada.mkdir(parents=True, exist_ok=True)

    season_nfo_dest = carpeta_temporada / "season.nfo"
    if not season_nfo_dest.exists():
        shutil.copy2(season_meta["season_nfo"], season_nfo_dest)

    poster_candidates = [
        season_meta["season_dir"] / "poster.png",
        season_meta["season_dir"] / "folder.jpg",
        season_meta["season_dir"] / "folder.png",
        season_meta["season_dir"] / "season.jpg",
        season_meta["season_dir"] / "season.png",
    ]
    for poster_src in poster_candidates:
        if poster_src.exists():
            poster_dest = carpeta_temporada / poster_src.name
            if not poster_dest.exists():
                shutil.copy2(poster_src, poster_dest)
            break

    return carpeta_temporada


def renombrar_y_copiar_nfo_segun_metadata(video_path: Path, destino_base: Path, indice_metadatos: dict, series_name: str, season_number: int) -> Path:
    log_debug(f"Procesando vídeo descargado: {video_path}")
    log_debug(f"season_number usada para metadatos: {season_number}")

    info = parsear_nombre_descargado(video_path.name)
    log_debug(f"Resultado parsear_nombre_descargado: {info}")

    if not info:
        log_error(f"No se pudo parsear el nombre del archivo descargado: {video_path.name}")
        return video_path

    season_meta = indice_metadatos.get(season_number)
    log_debug(f"season_meta encontrada: {season_meta is not None}")

    if not season_meta:
        log_error(f"No encontré metadatos para Season {season_number}")
        return video_path

    ep_nfo_src = season_meta["episodes"].get(info["episode_in_arc"])
    log_debug(f"Buscando episodio {info['episode_in_arc']} -> {ep_nfo_src}")

    if not ep_nfo_src:
        log_error(f"No encontré .nfo para Season {season_number} episodio {info['episode_in_arc']}")
        return video_path

    carpeta_temporada = asegurar_estructura_temporada(destino_base, season_meta, series_name)
    log_debug(f"Carpeta temporada asegurada: {carpeta_temporada}")

    nuevo_stem = ep_nfo_src.stem
    nuevo_video = carpeta_temporada / f"{nuevo_stem}{video_path.suffix}"
    nuevo_nfo = carpeta_temporada / ep_nfo_src.name

    log_debug(f"Nuevo vídeo destino: {nuevo_video}")
    log_debug(f"Nuevo nfo destino: {nuevo_nfo}")

    if video_path.resolve() != nuevo_video.resolve():
        if nuevo_video.exists():
            log_error(f"Ya existe destino de vídeo y no se sobrescribirá: {nuevo_video}")
        else:
            shutil.move(str(video_path), str(nuevo_video))
            log_debug(f"Vídeo movido a: {nuevo_video}")

    if not nuevo_nfo.exists():
        shutil.copy2(ep_nfo_src, nuevo_nfo)
        log_debug(f"NFO copiado a: {nuevo_nfo}")

    return nuevo_video


def contar_descargados_para_enlace(item_id: str, url: str, output_dir: Path, season_number: int, indice_metadatos: dict | None = None, series_name: str = "One Pace"):
    try:
        archivos = obtener_archivos_lista_pixeldrain(url)
    except Exception:
        return None, None

    disponibles = 0
    descargados = 0

    for archivo in archivos:
        file_id = archivo.get("id")
        if not file_id:
            continue
        disponibles += 1

        nombre = archivo.get("name") or f"{file_id}.bin"
        encontrado = False

        if indice_metadatos:
            info = parsear_nombre_descargado(nombre)
            if info:
                season_meta = indice_metadatos.get(season_number)
                if season_meta:
                    ep_nfo_src = season_meta["episodes"].get(info["episode_in_arc"])
                    if ep_nfo_src:
                        posible = output_dir / series_name / f"Season {season_meta['season_number']}" / f"{ep_nfo_src.stem}{Path(nombre).suffix}"
                        if posible.exists():
                            encontrado = True

        if not encontrado:
            carpeta_item = output_dir / "_tmp" / slugify(item_id)
            if (carpeta_item / nombre).exists():
                encontrado = True

        if encontrado:
            descargados += 1

    return descargados, disponibles


def listar_disponibles():
    config = cargar_config()

    if not validar_configuracion(config):
        raise SystemExit(1)

    url = config.get("url", DEFAULT_CONFIG["url"])
    output_dir = Path(config.get("output_dir", DEFAULT_CONFIG["output_dir"])).expanduser()
    metadata_dir = Path(config.get("metadata_dir", DEFAULT_CONFIG["metadata_dir"])).expanduser()
    series_name = config.get("series_name", DEFAULT_CONFIG["series_name"])
    quality_config = config.get("quality", "max").lower()

    indice_metadatos = construir_indice_metadatos(metadata_dir)

    print(f"Usando URL: {url}")
    print(f"Destino por defecto: {output_dir}")
    print(f"Metadatos: {metadata_dir}")
    print(f"Calidad configurada: {quality_config}")

    html = obtener_html(url)
    temporadas = extraer_temporadas_y_pixeldrain(html)
    agrupadas = agrupar_por_temporada(temporadas)

    print("\nListado disponible:\n")
    for arc in agrupadas:
        if not arc["opciones"]:
            print(f"{arc['season_number']}. {arc['id']} | sin enlaces disponibles | elegida: ninguna | -/-")
            continue

        disponibles = sorted(
            {x['quality'] for x in arc["opciones"] if x.get("quality") != "desconocida"},
            key=ordenar_calidades
        )

        elegida = elegir_opcion_por_calidad(arc["opciones"], quality_config)
        elegida_quality = elegida.get("quality", "desconocida") if elegida else "ninguna"

        descargados, total = (None, None)
        if elegida:
            descargados, total = contar_descargados_para_enlace(
                arc["id"],
                elegida["url"],
                output_dir,
                season_number=arc["season_number"],
                indice_metadatos=indice_metadatos,
                series_name=series_name,
            )

        estado = "?/?" if descargados is None else f"{descargados}/{total}"
        disponibles_txt = ", ".join(disponibles) if disponibles else "desconocida"

        print(f"{arc['season_number']}. {arc['id']} | disponible: {disponibles_txt} | elegida: {elegida_quality} | {estado}")


def procesar_url_pixeldrain(url: str, carpeta_base: Path, session: requests.Session):
    tipo, item_id = extraer_tipo_e_id(url)
    descargados = []

    if tipo == "file":
        info = pedir_json_resistente(f"/file/{item_id}/info", url)
        nombre = info.get("name") or f"{item_id}.bin"
        ruta = descargar_archivo_reanudable(item_id, nombre, carpeta_base, session, url)
        descargados.append(ruta)

    elif tipo == "list":
        data = pedir_json_resistente(f"/list/{item_id}", url)
        archivos = data.get("files", [])

        for archivo in archivos:
            file_id = archivo.get("id")
            if not file_id:
                continue

            nombre = archivo.get("name") or f"{file_id}.bin"
            ruta = descargar_archivo_reanudable(file_id, nombre, carpeta_base, session, url)
            descargados.append(ruta)

    return descargados


def descargar_desde_diccionario(items, carpeta_salida="descargas_pixeldrain", indice_metadatos=None, series_name="One Pace"):
    session = crear_sesion()
    resultados = []
    destino_base = Path(carpeta_salida)

    for item in items:
        item_id = item.get("id", "sin_id")
        season_number = item.get("season_number")
        carpeta_item_temporal = destino_base / "_tmp" / slugify(item_id)

        for enlace in item.get("pixeldrain", []):
            texto = enlace.get("texto", "")
            url = enlace.get("url")

            if not url:
                continue

            try:
                print(f"\nDescargando: {item_id} | {texto}")
                rutas_descargadas = procesar_url_pixeldrain(url, carpeta_item_temporal, session)

                rutas_finales = []
                for ruta in rutas_descargadas:
                    ruta_final = ruta
                    if indice_metadatos and season_number is not None:
                        ruta_final = renombrar_y_copiar_nfo_segun_metadata(
                            video_path=ruta,
                            destino_base=destino_base,
                            indice_metadatos=indice_metadatos,
                            series_name=series_name,
                            season_number=season_number,
                        )
                    rutas_finales.append(str(ruta_final))

                resultados.append({
                    "id": item_id,
                    "season_number": season_number,
                    "texto": texto,
                    "url": url,
                    "ok": True,
                    "archivos": rutas_finales,
                })
                print(f"[OK] {item_id} -> {len(rutas_finales)} archivo(s)")
            except Exception as e:
                resultados.append({
                    "id": item_id,
                    "season_number": season_number,
                    "texto": texto,
                    "url": url,
                    "ok": False,
                    "error": str(e),
                })
                log_error(f"{item_id} -> {e}")

    return resultados


def ejecutar_descarga():
    config = cargar_config()

    if not validar_configuracion(config):
        raise SystemExit(1)

    url = config.get("url", DEFAULT_CONFIG["url"])
    metadata_dir = Path(config.get("metadata_dir", DEFAULT_CONFIG["metadata_dir"])).expanduser()
    series_name = config.get("series_name", DEFAULT_CONFIG["series_name"])
    quality_config = config.get("quality", "max").lower()

    print(f"Usando URL: {url}")
    print(f"Destino por defecto: {config.get('output_dir')}")
    print(f"Metadatos: {metadata_dir}")
    print(f"Calidad configurada: {quality_config}")

    indice_metadatos = construir_indice_metadatos(metadata_dir)
    log_debug(f"Entradas indexadas en metadatos: {len(indice_metadatos)}", config)

    html = obtener_html(url)
    temporadas = extraer_temporadas_y_pixeldrain(html)
    agrupadas = agrupar_por_temporada(temporadas)

    opciones = []
    for arc in agrupadas:
        if not arc["opciones"]:
            texto = "sin enlaces disponibles"
        else:
            disponibles = sorted(
                {x['quality'] for x in arc["opciones"] if x.get("quality") != "desconocida"},
                key=ordenar_calidades
            )
            disponibles_txt = ", ".join(disponibles) if disponibles else "desconocida"
            texto = f"disponible: {disponibles_txt}"

        opciones.append({
            "n": arc["season_number"],
            "id": arc["id"],
            "texto": texto,
            "url": "",
        })

    seleccionadas = filtrar_opciones(opciones)

    if not seleccionadas:
        print("\nNo has seleccionado nada.")
        raise SystemExit(0)

    ids_seleccionados = {x["id"] for x in seleccionadas}
    agrupadas_filtradas = [x for x in agrupadas if x["id"] in ids_seleccionados]

    agrupadas_con_enlaces = [x for x in agrupadas_filtradas if x["opciones"]]

    if not agrupadas_con_enlaces:
        print("\nLas temporadas seleccionadas no tienen enlaces disponibles para descargar.")
        raise SystemExit(0)

    carpeta_destino = pedir_carpeta_destino(config)
    items_filtrados = reconstruir_items_segun_calidad(agrupadas_con_enlaces, quality_config)

    print(f"\nSe van a procesar {len(items_filtrados)} temporada(s).")
    print(f"Destino: {carpeta_destino.resolve()}")

    resultados = descargar_desde_diccionario(
        items_filtrados,
        carpeta_destino,
        indice_metadatos=indice_metadatos,
        series_name=series_name,
    )

    print("\nResumen:")
    print(json.dumps(resultados, indent=2, ensure_ascii=False))


def imprimir_ayuda():
    print("Uso:")
    print("  opdes")
    print("  opdes --run")
    print("  opdes --list")
    print("  opdes --check")
    print("  opdes --show_config")
    print("  opdes --set_url <url>")
    print("  opdes --set_output <ruta>")
    print("  opdes --set_metadata <ruta>")
    print("  opdes --set_quality <max|480p|720p|1080p>")
    print("  opdes --set_log_level <error|debug>")
    print("\nEjemplos:")
    print("  opdes --show_config")
    print("  opdes --set_url https://onepace.net/es/watch")
    print("  opdes --set_output ~/Downloads/OnePace")
    print("  opdes --set_metadata ./one-pace-jellyfin-master/One Pace")
    print("  opdes --set_quality max")
    print("  opdes --set_quality 720p")
    print("  opdes --set_log_level error")
    print("  opdes --set_log_level debug")
    print("  opdes --check")
    print("  opdes --list")
    print("  opdes --run")


def main():
    args = sys.argv[1:]

    if not args:
        ejecutar_descarga()
        return

    if "--show_config" in args:
        mostrar_config()
        return

    if "--set_url" in args:
        idx = args.index("--set_url")
        if idx + 1 >= len(args):
            print("Falta el valor para --set_url")
            return
        set_url(args[idx + 1])
        return

    if "--set_output" in args:
        idx = args.index("--set_output")
        if idx + 1 >= len(args):
            print("Falta el valor para --set_output")
            return
        set_output(args[idx + 1])
        return

    if "--set_metadata" in args:
        idx = args.index("--set_metadata")
        if idx + 1 >= len(args):
            print("Falta el valor para --set_metadata")
            return
        set_metadata(args[idx + 1])
        return

    if "--set_quality" in args:
        idx = args.index("--set_quality")
        if idx + 1 >= len(args):
            print("Falta el valor para --set_quality")
            return
        set_quality(args[idx + 1])
        return

    if "--set_log_level" in args:
        idx = args.index("--set_log_level")
        if idx + 1 >= len(args):
            print("Falta el valor para --set_log_level")
            return
        set_log_level(args[idx + 1])
        return

    if "--check" in args:
        config = cargar_config()
        ok = validar_configuracion(config)
        raise SystemExit(0 if ok else 1)

    if "--list" in args:
        listar_disponibles()
        return

    if "--run" in args:
        ejecutar_descarga()
        return

    imprimir_ayuda()


if __name__ == "__main__":
    main()