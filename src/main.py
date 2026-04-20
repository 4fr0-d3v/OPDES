# coding: utf-8

import json
import re
import sys
import time
from pathlib import Path
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import (
    ChunkedEncodingError,
    ConnectionError,
    Timeout,
    RequestException,
    HTTPError,
)
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from tqdm import tqdm


CONFIG_PATH = Path(__file__).resolve().parent / "config.json"

DEFAULT_CONFIG = {
    "url": "https://onepace.net/es/watch",
    "output_dir": "descargas_pixeldrain",
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
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2, ensure_ascii=False)


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
            print(f"  [reintento HTML {intento}/5] {url} -> {e}")
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

        if pixeldrain_links:
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
                print(f"  [reintento JSON {intento}/{max_intentos}] {url} -> {e}")
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
        print(f"  [skip] Ya existe: {destino}")
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
                print(f"  [reintento descarga {intento}/{MAX_REINTENTOS_DESCARGA}] {file_id} -> {e}")
                time.sleep(espera)

            except RuntimeError:
                raise

    raise RuntimeError(
        f"Fallo descargando {file_id} tras {MAX_REINTENTOS_DESCARGA} intentos por dominio: {ultimo_error}"
    )


def aplanar_items(items: list[dict]) -> list[dict]:
    planos = []
    idx = 1

    for item in items:
        item_id = item.get("id", "sin_id")
        for enlace in item.get("pixeldrain", []):
            planos.append({
                "n": idx,
                "id": item_id,
                "texto": enlace.get("texto", ""),
                "url": enlace.get("url", ""),
            })
            idx += 1

    return planos


def mostrar_opciones(opciones: list[dict]):
    print("\nEnlaces encontrados:\n")
    for op in opciones:
        print(f"{op['n']:>3}. {op['id']} | {op['texto']} | {op['url']}")


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
    print("  - filtro por texto: text:720p")
    print("  - filtro combinado simple: arc:romance text:1080p")

    entrada = input("\nQué quieres descargar: ").strip()

    if not entrada:
        return opciones

    entrada_lower = entrada.lower()

    if entrada_lower in {"*", "all", "todo", "todos"}:
        return opciones

    tokens = entrada_lower.split()
    arc_filters = [t[4:] for t in tokens if t.startswith("arc:")]
    text_filters = [t[5:] for t in tokens if t.startswith("text:")]

    if arc_filters or text_filters:
        resultado = opciones[:]
        for arc in arc_filters:
            resultado = [x for x in resultado if arc in x["id"].lower()]
        for txt in text_filters:
            resultado = [x for x in resultado if txt in x["texto"].lower()]
        return resultado

    indices = parsear_seleccion(entrada, len(opciones))
    return [x for x in opciones if x["n"] in indices]


def pedir_carpeta_destino(config: dict) -> Path:
    default_dir = config.get("output_dir", DEFAULT_CONFIG["output_dir"])
    entrada = input(f"\nCarpeta de destino (vacío = {default_dir}): ").strip()
    if not entrada:
        return Path(default_dir).expanduser()
    return Path(entrada).expanduser()


def reconstruir_items(opciones_filtradas: list[dict]) -> list[dict]:
    agrupado = {}

    for op in opciones_filtradas:
        item_id = op["id"]
        agrupado.setdefault(item_id, [])
        agrupado[item_id].append({
            "texto": op["texto"],
            "url": op["url"],
        })

    return [{"id": k, "pixeldrain": v} for k, v in agrupado.items()]


def obtener_archivos_lista_pixeldrain(url: str) -> list[dict]:
    tipo, item_id = extraer_tipo_e_id(url)

    if tipo == "file":
        info = pedir_json_resistente(f"/file/{item_id}/info", url)
        return [info]

    data = pedir_json_resistente(f"/list/{item_id}", url)
    return data.get("files", [])


def contar_descargados_para_enlace(item_id: str, url: str, output_dir: Path):
    carpeta_item = output_dir / slugify(item_id)

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
        if (carpeta_item / nombre).exists():
            descargados += 1

    return descargados, disponibles


def listar_disponibles():
    config = cargar_config()
    url = config.get("url", DEFAULT_CONFIG["url"])
    output_dir = Path(config.get("output_dir", DEFAULT_CONFIG["output_dir"])).expanduser()

    print(f"Usando URL: {url}")
    print(f"Destino por defecto: {output_dir}")

    html = obtener_html(url)
    temporadas = extraer_temporadas_y_pixeldrain(html)
    opciones = aplanar_items(temporadas)

    print("\nListado disponible:\n")
    session = crear_sesion()
    _ = session  # mantenido por coherencia con el resto del script

    for op in opciones:
        descargados, disponibles = contar_descargados_para_enlace(op["id"], op["url"], output_dir)

        if descargados is None:
            estado = "?/?"
        else:
            estado = f"{descargados}/{disponibles}"

        print(f"{op['n']}. {op['id']} | {op['texto']} | {op['url']} | {estado}")


def procesar_url_pixeldrain(url: str, carpeta_base: Path, session: requests.Session):
    tipo, item_id = extraer_tipo_e_id(url)
    descargados = []

    if tipo == "file":
        info = pedir_json_resistente(f"/file/{item_id}/info", url)
        nombre = info.get("name") or f"{item_id}.bin"
        ruta = descargar_archivo_reanudable(item_id, nombre, carpeta_base, session, url)
        descargados.append(str(ruta))

    elif tipo == "list":
        data = pedir_json_resistente(f"/list/{item_id}", url)
        archivos = data.get("files", [])

        print(
            f"  [debug] lista {item_id}: "
            f"file_count={data.get('file_count')} "
            f"archivos_recibidos={len(archivos)}"
        )

        for archivo in archivos:
            file_id = archivo.get("id")
            if not file_id:
                continue

            nombre = archivo.get("name") or f"{file_id}.bin"
            ruta = descargar_archivo_reanudable(file_id, nombre, carpeta_base, session, url)
            descargados.append(str(ruta))

    return descargados


def descargar_desde_diccionario(items, carpeta_salida="descargas_pixeldrain"):
    session = crear_sesion()
    resultados = []

    for item in items:
        item_id = item.get("id", "sin_id")
        carpeta_item = Path(carpeta_salida) / slugify(item_id)

        for enlace in item.get("pixeldrain", []):
            texto = enlace.get("texto", "")
            url = enlace.get("url")

            if not url:
                continue

            try:
                print(f"\nDescargando: {item_id} | {texto}")
                descargados = procesar_url_pixeldrain(url, carpeta_item, session)
                resultados.append({
                    "id": item_id,
                    "texto": texto,
                    "url": url,
                    "ok": True,
                    "archivos": descargados,
                })
                print(f"[OK] {item_id} -> {len(descargados)} archivo(s)")
            except Exception as e:
                resultados.append({
                    "id": item_id,
                    "texto": texto,
                    "url": url,
                    "ok": False,
                    "error": str(e),
                })
                print(f"[ERROR] {item_id} -> {e}")

    return resultados


def ejecutar_descarga():
    config = cargar_config()
    url = config.get("url", DEFAULT_CONFIG["url"])

    print(f"Usando URL: {url}")
    print(f"Destino por defecto: {config.get('output_dir')}")

    html = obtener_html(url)
    temporadas = extraer_temporadas_y_pixeldrain(html)

    opciones = aplanar_items(temporadas)
    seleccionadas = filtrar_opciones(opciones)

    if not seleccionadas:
        print("\nNo has seleccionado nada.")
        raise SystemExit(0)

    carpeta_destino = pedir_carpeta_destino(config)
    items_filtrados = reconstruir_items(seleccionadas)

    print(f"\nSe van a procesar {len(seleccionadas)} enlace(s).")
    print(f"Destino: {carpeta_destino.resolve()}")

    resultados = descargar_desde_diccionario(items_filtrados, carpeta_destino)

    print("\nResumen:")
    print(json.dumps(resultados, indent=2, ensure_ascii=False))


def imprimir_ayuda():
    print("Uso:")
    print("  python main.py")
    print("  python main.py --run")
    print("  python main.py --list")
    print("  python main.py --show_config")
    print("  python main.py --set_url <url>")
    print("  python main.py --set_output <ruta>")
    print("\nEjemplos:")
    print("  python main.py --show_config")
    print("  python main.py --set_url https://onepace.net/es/watch")
    print("  python main.py --set_output ~/Downloads/OnePace")
    print("  python main.py --list")
    print("  python main.py --run")


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

    if "--list" in args:
        listar_disponibles()
        return

    if "--run" in args:
        ejecutar_descarga()
        return

    imprimir_ayuda()


if __name__ == "__main__":
    main()