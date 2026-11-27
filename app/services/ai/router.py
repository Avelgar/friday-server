# -*- coding: utf-8 -*-
import os
import re
import json
import struct
import zipfile
import asyncio
import logging
import mimetypes
import urllib.parse
import xml.etree.ElementTree as ET
from google.genai import types
from .key_pool import (
    GeminiKeyPool,
    create_genai_client,
    is_transient_error,
    run_generate_content_in_thread,
)
from .model_config import get_models_config, to_api_model
from .prompts import DIRECT_SYSTEM_INSTRUCTION, PLANNER_SYSTEM_INSTRUCTION

logger = logging.getLogger("AI_Service")

DIRECT_HISTORY_CHAR_BUDGET = 12000
ANTIGRAVITY_HISTORY_CHAR_BUDGET = 5000
PLANNER_FILES_CHAR_BUDGET = 70_000
# На повторных попытках (после 503/504) режем контекст: так запрос реально
# успевает в дедлайн Google, а не просто повторяет ту же «тяжёлую» ошибку.
PLANNER_RETRY_CHAR_BUDGET = 25_000
# Таймауты планировщика: http-таймаут клиента и «жёсткий» asyncio-дедлайн (больше него).
PLANNER_HTTP_TIMEOUT_MS = 120_000
PLANNER_WAIT_TIMEOUT_SEC = 125.0
PLANNER_ATTEMPTS = 3
PLANNER_RETRY_DELAYS = (3.0, 8.0)
UPLOADS_DIR = "/opt/friday/storage/uploads"
IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".webp", ".gif"}
CALL_PREFIXES = ("[CALL_PLANNER", "[CALL_ANTIGRAVITY", "[ESCALATE_ANTIGRAVITY")
# DECIDE_PREFIXES — теги, которые DIRECT разбирает сам, а не отправляет в чат
# (иначе «Действие: cmd.exe /d /s /c chcp 65001 && curl …» уходит как команда IDE).
DECIDE_PREFIXES = CALL_PREFIXES + ("[RUN_COMMAND", "[GET_FILES", "[GET_TREE", "[DELETE:")


def _compress_history_entries(
    recent_history: list | None,
    char_budget: int,
    max_items: int = 12,
    user_agent_cap: int = 1800,
    latest_tool_cap: int = 3000,
    old_tool_cap: int = 450,
) -> list[tuple[str, str]]:
    if not recent_history:
        return []

    tail = recent_history[-max_items:]
    collected: list[tuple[str, str]] = []
    used_chars = 0
    seen_latest_tool = False

    for m in reversed(tail):
        r = m.get("role") or "user"
        txt = (m.get("text") or "").strip()
        if not txt:
            continue

        if r in ("tool", "system"):
            cap = old_tool_cap if seen_latest_tool else latest_tool_cap
            seen_latest_tool = True
        else:
            cap = user_agent_cap

        if len(txt) > cap:
            txt = txt[:cap] + "\n...[обрезано]..."

        if used_chars + len(txt) > char_budget and collected:
            break

        collected.append((r, txt))
        used_chars += len(txt)

    collected.reverse()
    return collected


def format_manifest_block(
    file_manifest: list | None,
    selected_files: list | None = None,
    with_urls: bool = True,
) -> str:
    if not file_manifest:
        return "Файлов на сервере пока нет."

    selected_norm = {
        re.sub(r"^(\./)+", "", f.strip().replace("\\", "/")).lstrip("/")
        for f in (selected_files or [])
        if f and f.strip()
    }

    ordered = list(file_manifest)
    if selected_norm:
        priority = [f for f in ordered if f["path"] in selected_norm]
        others = [f for f in ordered if f["path"] not in selected_norm]
        ordered = priority + others[:6]
    else:
        ordered = ordered[:15]

    lines = []
    for f in ordered:
        size_kb = f["size"] / 1024.0
        size_str = f"{size_kb:.1f} КБ" if size_kb >= 0.1 else f"{f['size']} Б"
        lines_info = f", ~{f['lines']} строк" if f.get("lines") is not None else ""
        mark = " [ЦЕЛЕВОЙ ФАЙЛ]" if f["path"] in selected_norm else ""
        if with_urls:
            lines.append(f"• {f['path']}{mark} ({size_str}{lines_info}) | curl: {f['url']}")
        else:
            lines.append(f"• {f['path']}{mark} ({size_str}{lines_info})")
    return "\n".join(lines)


def format_stateless_prompt_parts(
    prompt_text: str,
    recent_history: list = None,
    project_tree: str = None,
    file_manifest: list = None,
    selected_files: list = None,
) -> dict:
    tree_block = ""
    if project_tree and project_tree.strip():
        tree_txt = project_tree.strip()
        if len(tree_txt) > 4000:
            tree_txt = tree_txt[:4000] + "\n...[дерево обрезано]..."
        tree_block = f"# Дерево проекта:\n{tree_txt}"

    manifest_txt = format_manifest_block(file_manifest, selected_files=selected_files, with_urls=True)
    manifest_block = f"# Файлы на сервере:\n{manifest_txt}"

    history_block = ""
    entries = _compress_history_entries(recent_history, char_budget=ANTIGRAVITY_HISTORY_CHAR_BUDGET, max_items=6)
    if entries:
        lines = []
        for r, txt in entries:
            label = "Пользователь" if r == "user" else ("IDE" if r in ("system", "tool") else "Ассистент")
            lines.append(f"[{label}]: {txt}")
        history_block = "# Недавний контекст:\n" + "\n\n".join(lines)

    task_block = f"# Задача этого шага:\n{prompt_text}"

    sections = [b for b in (tree_block, manifest_block, history_block, task_block) if b]
    return {
        "full_text": "\n\n".join(sections),
        "tree": tree_block,
        "manifest": manifest_block,
        "history": history_block,
        "task": task_block,
    }


def _resolve_disk_path(f: dict) -> str | None:
    dp = f.get("disk_path")
    if dp and os.path.isfile(dp):
        return dp
    url = f.get("url") or ""
    m = re.search(r"/uploads/([^/]+)/([^?]+)", url)
    if m:
        c_uid = os.path.basename(urllib.parse.unquote(m.group(1)))
        rel = urllib.parse.unquote(m.group(2)).lstrip("/")
        base = os.path.abspath(os.path.join(UPLOADS_DIR, c_uid))
        cand = os.path.abspath(os.path.join(base, rel))
        if cand.startswith(base + os.sep) and os.path.isfile(cand):
            return cand
    return None


def _summarize_stl(disk_path: str, rel_path: str) -> str:
    size = os.path.getsize(disk_path)
    with open(disk_path, "rb") as f:
        header = f.read(84)
        if len(header) == 84:
            tri_count = struct.unpack("<I", header[80:84])[0]
            if tri_count > 0 and (84 + tri_count * 50) == size:
                min_x = min_y = min_z = float("inf")
                max_x = max_y = max_z = float("-inf")
                for _ in range(min(tri_count, 500_000)):
                    rec = f.read(50)
                    if len(rec) < 50:
                        break
                    vals = struct.unpack("<12fH", rec)
                    for vi in (3, 6, 9):
                        x, y, z = vals[vi], vals[vi + 1], vals[vi + 2]
                        if x < min_x: min_x = x
                        if x > max_x: max_x = x
                        if y < min_y: min_y = y
                        if y > max_y: max_y = y
                        if z < min_z: min_z = z
                        if z > max_z: max_z = z
                return (
                    f"[Binary STL: {rel_path} | размер={size} байт | треугольников={tri_count}]\n"
                    f"Габариты (Bounding Box):\n"
                    f"  X: [{min_x:.3f} .. {max_x:.3f}] (размер {max_x - min_x:.3f})\n"
                    f"  Y: [{min_y:.3f} .. {max_y:.3f}] (размер {max_y - min_y:.3f})\n"
                    f"  Z: [{min_z:.3f} .. {max_z:.3f}] (размер {max_z - min_z:.3f})"
                )

    min_x = min_y = min_z = float("inf")
    max_x = max_y = max_z = float("-inf")
    tri_count = 0
    with open(disk_path, "r", encoding="utf-8", errors="replace") as tf:
        for line in tf:
            s = line.strip()
            if s.startswith("facet normal"):
                tri_count += 1
            elif s.startswith("vertex "):
                parts = s.split()
                if len(parts) >= 4:
                    try:
                        x, y, z = float(parts[1]), float(parts[2]), float(parts[3])
                        if x < min_x: min_x = x
                        if x > max_x: max_x = x
                        if y < min_y: min_y = y
                        if y > max_y: max_y = y
                        if z < min_z: min_z = z
                        if z > max_z: max_z = z
                    except ValueError:
                        pass
    if tri_count > 0 and min_x != float("inf"):
        return (
            f"[ASCII STL: {rel_path} | размер={size} байт | треугольников={tri_count}]\n"
            f"Габариты (Bounding Box):\n"
            f"  X: [{min_x:.3f} .. {max_x:.3f}] (размер {max_x - min_x:.3f})\n"
            f"  Y: [{min_y:.3f} .. {max_y:.3f}] (размер {max_y - min_y:.3f})\n"
            f"  Z: [{min_z:.3f} .. {max_z:.3f}] (размер {max_z - min_z:.3f})"
        )
    return f"[STL файл {rel_path}, {size} байт]"


def _read_numbered_file_content(disk_path: str, rel_path: str) -> str | None:
    if not disk_path or not os.path.isfile(disk_path):
        return None
    ext = os.path.splitext(rel_path)[1].lower()
    try:
        if ext == ".stl":
            return _summarize_stl(disk_path, rel_path)
        if ext == ".docx":
            with zipfile.ZipFile(disk_path, "r") as z:
                xml_bytes = z.read("word/document.xml")
            root = ET.fromstring(xml_bytes)
            lines = ["".join(p.itertext()) for p in root.iter() if p.tag.endswith("}p")]
        else:
            with open(disk_path, "rb") as f:
                raw = f.read()
            if b"\x00" in raw[:4096]:
                return f"[Бинарный файл {rel_path}, {len(raw)} байт]"
            lines = raw.decode("utf-8", errors="replace").splitlines()
        return "\n".join(f"{idx}: {line}" for idx, line in enumerate(lines, start=1))
    except Exception as e:
        return f"[Не удалось прочитать {rel_path}: {e}]"


def build_direct_contents(prompt_text: str, recent_history: list = None) -> list[types.Content]:
    turns: list[tuple[str, str]] = []
    entries = _compress_history_entries(recent_history, char_budget=DIRECT_HISTORY_CHAR_BUDGET)

    for r, txt in entries:
        if r == "agent":
            role = "model"
        elif r in ("tool", "system"):
            role = "user"
            txt = f"[Событие IDE]: {txt}"
        else:
            role = "user"

        if turns and turns[-1][0] == role:
            turns[-1] = (role, turns[-1][1] + "\n\n" + txt)
        else:
            turns.append((role, txt))

    while turns and turns[0][0] == "model":
        turns.pop(0)

    if turns and turns[-1][0] == "user":
        turns[-1] = ("user", turns[-1][1] + "\n\n" + prompt_text)
    else:
        turns.append(("user", prompt_text))

    return [
        types.Content(role=role, parts=[types.Part.from_text(text=text)])
        for role, text in turns
    ]


_SYNC_TAG_RE = re.compile(r"\[SYNC:\s*([^\]]+)\]", re.IGNORECASE)


# Модель иногда открывает тег разговорным префиксом («Сейчас выполню: [CALL…»).
_FILLER_PREFIX_RE = re.compile(
    r"^[^\[]{0,120}?[:\-—]\s*(?=\[(?:CALL_|ESCALATE_|GET_FILES|GET_TREE|SYNC:))",
    re.DOTALL,
)


def _norm_tag_start(text: str) -> str:
    """Срезает «Сейчас сделаю:» перед тегом, чтобы стрим не считал это обычным ответом."""
    t = (text or "").lstrip()
    if t.startswith("["):
        return t
    m = _FILLER_PREFIX_RE.match(t)
    return t[m.end():] if m else t


def _sync_paths_exist(files: list[str], file_manifest: list | None) -> bool:
    """Для прямых (DIRECT) правок: все [SYNC: ...] в ответе — это реальные файлы на сервере."""
    if not files or not file_manifest:
        return False
    known = {f["path"] for f in file_manifest}
    return all(f in known for f in files)


def _inject_sync_tag(text: str, files: list[str]) -> str:
    """Добавляет [SYNC: путь] к ответу, где модель забыла тег (иначе правки не уедут на ПК)."""
    missing = [f for f in files if not re.search(rf"\[SYNC:\s*{re.escape(f)}\s*\]", text, re.IGNORECASE)]
    if not missing:
        return text
    block = "\n".join(f"[SYNC: {p}]" for p in missing)
    return f"{block}\n\n{text}".strip()


# В подписи блока путь может содержать '/': ```js/entities.js — разрешаем его.
_CODE_FENCE_RE = re.compile(r"```([^\s`]*)[ \t]*\n(.*?)(?:```|\Z)", re.DOTALL)
# Если модель переписала почти весь файл — считаем это полной заменой.
_FULL_REWRITE_MIN_LINES = 40


def _find_manifest_entry(rel_path: str, file_manifest: list | None) -> dict | None:
    for f in (file_manifest or []):
        if f.get("path") == rel_path:
            return f
    return None


def _apply_code_patch(text: str, rel_path: str, file_manifest: list | None = None) -> str | None:
    """
    Применяет ```-блок из ответа DIRECT к файлу на сервере.

    Два формата, которые модель реально выдаёт:
      • полная перезапись — блок подписан путём/именем файла (```js/entities.js)
        или содержит почти весь файл;
      • фрагмент — контекстные строки «как в файле» + строки с '+' (как в diff).
    Возвращает новое содержимое или None (тогда ответ остаётся обычным текстом).
    """
    blocks = [(m.group(1) or "", m.group(2)) for m in _CODE_FENCE_RE.finditer(text or "")]
    if not blocks:
        return None

    entry = _find_manifest_entry(rel_path, file_manifest)
    disk_p = _resolve_disk_path(entry) if entry else None
    if not disk_p:
        return None
    try:
        with open(disk_p, "r", encoding="utf-8", errors="replace") as fh:
            current = fh.read()
    except Exception:
        return None

    current_lines = current.splitlines()
    fname = os.path.basename(rel_path)

    def _named(lang: str) -> bool:
        lang_l = (lang or "").strip().lower()
        return bool(lang_l) and (rel_path.lower() in lang_l or fname.lower() in lang_l)

    # 1) Фрагмент с '+'-строками (самый частый случай для мелких правок).
    ordered = [b for b in blocks if _named(b[0])] + [b for b in blocks if not _named(b[0])]
    for _lang, body in ordered:
        raw_lines = body.split("\n")
        segments: list[tuple[str, str]] = []
        for line in raw_lines:
            if line.startswith("+++") or line.startswith("---"):
                continue
            if line.startswith("+"):
                segments.append(("add", line[1:]))
            elif line.strip():
                segments.append(("ctx", line))
        added = [t for k, t in segments if k == "add"]
        anchors = [t for k, t in segments if k == "ctx"]
        if not added or not anchors:
            continue

        # Контекстные строки должны идти подряд в текущем файле.
        match_at = None
        for i in range(len(current_lines) - len(anchors) + 1):
            if all(current_lines[i + j].rstrip() == anchors[j].rstrip() for j in range(len(anchors))):
                match_at = i
                break
        if match_at is None:
            continue

        replacement: list[str] = []
        file_i = match_at
        for kind, line_text in segments:
            if kind == "ctx":
                replacement.append(current_lines[file_i])
                file_i += 1
            else:
                replacement.append(line_text)
        new_lines = current_lines[:match_at] + replacement + current_lines[file_i:]
        return "\n".join(new_lines) + ("\n" if current.endswith("\n") else "")

    # 2) Полная перезапись: блок подписан этим файлом.
    for lang, body in blocks:
        if _named(lang):
            return body if body.endswith("\n") else body + "\n"

    # 3) Один большой безымянный блок — считаем, что это новый файл целиком.
    big = [b for _l, b in blocks if len(b.splitlines()) >= _FULL_REWRITE_MIN_LINES]
    if len(big) == 1:
        body = big[0]
        return body if body.endswith("\n") else body + "\n"

    return None


def parse_direct_edit(text: str, file_manifest: list | None = None) -> dict | None:
    """
    Небольшая правка без планировщика: модель прислала патч в ```-блоке и
    [SYNC: путь] для файла, который уже лежит на сервере.

    Такой ответ раньше уходил в чат как обычный текст: файл на сервере не менялся,
    на ПК ничего не приезжало, а модель на следующем шаге начинала «скачивать
    curl'ом» уже имеющиеся файлы.
    """
    s = (text or "").strip()
    if not s or "```" not in s:
        return None
    sync_files = [m.group(1).strip() for m in _SYNC_TAG_RE.finditer(s)]
    if not sync_files:
        return None
    sync_files = [
        re.sub(r"^(\./)+", "", f.replace("\\", "/")).lstrip("/")
        for f in sync_files
    ]
    if not _sync_paths_exist(sync_files, file_manifest):
        return None
    return {"sync_files": sync_files}



def parse_antigravity_call(text: str) -> dict | None:
    s = (text or "").strip()
    if not s:
        return None

    # Старые/галлюцинированные теги: [CALL_ANTIGRAVITY ...], [ESCALATE_ANTIGRAVITY ...],
    # а также [CALL_PLANNER] без двоеточия — иначе DIRECT «отвечал» служебной строкой
    # в чат, и задача молча не запускалась.
    if re.search(r"\[ESCALATE_ANTIGRAVITY\b", s, flags=re.IGNORECASE):
        return {"task": None, "files": [], "network": False}

    short_form = re.search(r"\[CALL_(?:PLANNER|ANTIGRAVITY)\s*\]", s, flags=re.IGNORECASE)
    if short_form:
        return {"task": None, "files": [], "network": False}

    task_m = re.search(r"\[CALL_(?:PLANNER|ANTIGRAVITY)\s*:\s*(.+?)\]", s, flags=re.DOTALL | re.IGNORECASE)
    if not task_m:
        return None

    body = task_m.group(1).strip()
    need_network = False
    files: list[str] = []

    net_m = re.search(r"\|\s*network\s*:\s*([^\|\]]+)", body, flags=re.IGNORECASE)
    if net_m:
        val = net_m.group(1).strip().lower()
        need_network = val in ("да", "yes", "true", "1", "enabled")
        body = body[:net_m.start()] + body[net_m.end():]

    files_m = re.search(r"\|\s*files\s*:\s*([^\|\]]+)", body, flags=re.IGNORECASE)
    if files_m:
        raw_files = files_m.group(1).strip()
        if raw_files.lower() not in ("нет", "none", "-", "пусто", ""):
            files = [
                re.sub(r"^(\./)+", "", f.strip().replace("\\", "/")).lstrip("/")
                for f in raw_files.split(",")
                if f.strip()
            ]
        body = body[:files_m.start()] + body[files_m.end():]

    task = body.strip(" |\n\t")
    return {"task": task or None, "files": files, "network": need_network}


def _could_be_call_tag(text_upper: str) -> bool:
    compact = "".join(text_upper.split())
    return any(
        p.startswith(compact) or compact.startswith(p)
        for p in DECIDE_PREFIXES
    )


async def run_direct(
    key_pool: GeminiKeyPool,
    api_keys: list[str],
    prompt_text: str,
    recent_history: list = None,
    project_tree: str = None,
    file_manifest: list = None,
    status_callback=None,
    chunk_callback=None,
    assigned_key: int = None,
    per_request_key: bool = True,
    **kwargs,
):
    contents = build_direct_contents(prompt_text, recent_history=recent_history)

    context_sections = [DIRECT_SYSTEM_INSTRUCTION]
    if project_tree and project_tree.strip():
        tree_txt = project_tree.strip()
        if len(tree_txt) > 5000:
            tree_txt = tree_txt[:5000] + "\n...[дерево обрезано]..."
        context_sections.append(f"ДЕРЕВО ПРОЕКТА:\n{tree_txt}")
    else:
        context_sections.append("ДЕРЕВО ПРОЕКТА: отсутствует (при необходимости запроси [GET_TREE]).")

    manifest_txt = format_manifest_block(file_manifest, with_urls=False)
    context_sections.append(f"ФАЙЛЫ НА СЕРВЕРЕ:\n{manifest_txt}")
    full_system_instruction = "\n\n".join(context_sections)

    cfg = get_models_config()
    model_name = to_api_model(cfg["direct_model"])

    is_25 = "2.5" in model_name
    thinking_cfg = types.ThinkingConfig(thinking_budget=0) if is_25 else types.ThinkingConfig(thinking_level="low")
    afc_cfg = types.AutomaticFunctionCallingConfig(disable=True) if hasattr(types, "AutomaticFunctionCallingConfig") else None
    config_kwargs = {
        "system_instruction": full_system_instruction,
        "max_output_tokens": 8192,
        "thinking_config": thinking_cfg,
    }
    if afc_cfg is not None:
        config_kwargs["automatic_function_calling"] = afc_cfg
    config = types.GenerateContentConfig(**config_kwargs)

    # Каждый новый запрос выбирает самый свободный ключ пула (assigned_key — только подсказка).
    if per_request_key:
        key_idx = key_pool.pick_key_for_request(model=model_name, avoid_index=assigned_key)
    elif assigned_key is not None and key_pool.is_usable(assigned_key, model=model_name):
        key_idx = assigned_key
    else:
        key_idx = key_pool.get_best_key(model=model_name)

    client = create_genai_client(api_keys, key_idx, timeout=25000)

    flushed_any = False
    early_buf = ""
    full_parts: list[str] = []
    usage_info = {"input": 0, "cached": 0, "output": 0, "total": 0}

    try:
        if status_callback:
            short_m = model_name.replace("models/", "")
            await status_callback({"status": f"Direct ({short_m}, ключ #{key_idx})..."})

        stream = await asyncio.wait_for(
            client.aio.models.generate_content_stream(
                model=model_name,
                contents=contents,
                config=config,
            ),
            timeout=20.0
        )

        async for chunk in stream:
            um = getattr(chunk, "usage_metadata", None)
            if um:
                usage_info = {
                    "input": int(getattr(um, "prompt_token_count", 0) or 0),
                    "cached": int(getattr(um, "cached_content_token_count", 0) or 0),
                    "output": int(getattr(um, "candidates_token_count", 0) or 0),
                    "total": int(getattr(um, "total_token_count", 0) or 0),
                }

            delta = ""
            try:
                delta = chunk.text or ""
            except Exception:
                delta = ""
            if not delta:
                continue

            full_parts.append(delta)

            if not flushed_any:
                early_buf += delta
                stripped_up = _norm_tag_start(early_buf).upper()
                if stripped_up and not stripped_up.startswith("["):
                    flushed_any = True
                    if chunk_callback:
                        await chunk_callback(early_buf)
                elif stripped_up and not _could_be_call_tag(stripped_up):
                    flushed_any = True
                    if chunk_callback:
                        await chunk_callback(early_buf)
            else:
                if chunk_callback:
                    await chunk_callback(delta)

        full_text = "".join(full_parts).strip()

        call_info = parse_antigravity_call(full_text)
        if call_info is not None:
            key_pool.mark_success(key_idx, model=model_name)
            logger.info(f"⚡ [DIRECT {model_name}] Ключ #{key_idx} | in={usage_info['input']} out={usage_info['output']} total={usage_info['total']} -> PLANNER")
            return {
                "escalate": True,
                "ag_task": call_info["task"],
                "ag_files": call_info["files"],
                "need_network": call_info["network"],
                "assigned_key": key_idx,
            }

        # Мелкая правка готового файла прямо в DIRECT: модель прислала патч в ```-блоке
        # и [SYNC: путь]. Раньше это уходило в чат как текст (правка терялась), а на
        # следующем шаге модель просила «скачать» уже имеющиеся файлы.
        edit_info = parse_direct_edit(full_text, file_manifest)
        if edit_info:
            patched = {}
            applied = []
            for rel_p in edit_info["sync_files"]:
                new_src = _apply_code_patch(text=full_text, rel_path=rel_p, file_manifest=file_manifest)
                if new_src is not None:
                    patched[rel_p] = new_src
                    applied.append(rel_p)
            if applied:
                key_pool.mark_success(key_idx, model=model_name)
                logger.info(
                    f"🩹 [DIRECT EDIT] Ключ #{key_idx}: правка без песочницы -> {', '.join(applied)}"
                )
                return {
                    "escalate": False,
                    "text": full_text,
                    "file_urls": {},
                    "direct_edits": patched,
                    "assigned_key": key_idx,
                    "usage": usage_info,
                }

        if not flushed_any and early_buf and chunk_callback:
            await chunk_callback(early_buf)

        key_pool.mark_success(key_idx, model=model_name)
        logger.info(f"⚡ [DIRECT {model_name}] Ключ #{key_idx} | in={usage_info['input']} cached={usage_info['cached']} out={usage_info['output']} total={usage_info['total']}")

        return {
            "escalate": False,
            "text": full_text,
            "file_urls": {},
            "assigned_key": key_idx,
            "usage": usage_info,
        }

    except Exception as e:
        logger.warning(f"⚠️ [DIRECT {model_name}] Ошибка на ключе #{key_idx}: {e}")
        if flushed_any:
            return {
                "escalate": False,
                "text": "".join(full_parts).strip(),
                "file_urls": {},
                "assigned_key": key_idx,
                "usage": usage_info,
            }
        key_pool.register_key_error(key_idx, model=model_name, exc=e)
        raise
    finally:
        try:
            if hasattr(client, "aio") and hasattr(client.aio, "aclose"):
                await client.aio.aclose()
        except Exception:
            pass


async def run_planner(
    key_pool: GeminiKeyPool,
    api_keys: list[str],
    task_prompt: str,
    recent_history: list = None,
    project_tree: str = None,
    file_manifest: list = None,
    selected_files: list = None,
    status_callback=None,
    assigned_key: int = None,
    per_request_key: bool = True,
) -> dict:
    """Читает файлы (и скриншоты) с диска и строит пошаговый план через модель Планировщика (250K TPM)."""
    cfg = get_models_config()
    planner_model = to_api_model(cfg["router_model"])
    # Планировщик берёт самый свободный ключ на момент запроса, а не ключ чата.
    if per_request_key:
        key_idx = key_pool.pick_key_for_request(model=planner_model, avoid_index=assigned_key)
    elif assigned_key is not None and key_pool.is_usable(assigned_key, model=planner_model):
        key_idx = assigned_key
    else:
        key_idx = key_pool.get_best_key(model=planner_model)

    selected_norm = {
        re.sub(r"^(\./)+", "", f.strip().replace("\\", "/")).lstrip("/")
        for f in (selected_files or [])
        if f and f.strip()
    }

    ordered_files = list(file_manifest or [])
    if selected_norm:
        ordered_files.sort(key=lambda f: (0 if f["path"] in selected_norm else 1, -f.get("mtime", 0)))

    def _build_planner_messages(budget: int, shrink_images: bool = False) -> tuple[list, list]:
        """Собирает (file_blocks, image_parts) под заданный символьный бюджет."""
        blocks: list[str] = []
        imgs: list = []
        used = 0

        for f in ordered_files:
            disk_p = _resolve_disk_path(f)
            if not disk_p:
                continue

            ext = os.path.splitext(f["path"])[1].lower()
            if ext in IMAGE_EXTS:
                if not shrink_images and len(imgs) < 3 and os.path.getsize(disk_p) <= 10 * 1024 * 1024:
                    try:
                        with open(disk_p, "rb") as imgf:
                            img_bytes = imgf.read()
                        mime, _ = mimetypes.guess_type(disk_p)
                        imgs.append(types.Part.from_bytes(data=img_bytes, mime_type=mime or "image/png"))
                        blocks.append(f"=== ИЗОБРАЖЕНИЕ: {f['path']} (передано в запрос как картинка) ===")
                    except Exception:
                        pass
                continue

            numbered = _read_numbered_file_content(disk_p, f["path"])
            if not numbered:
                continue
            block = f"=== ФАЙЛ: {f['path']} ===\n{numbered}"
            if used + len(block) > budget:
                if not blocks:
                    block = block[:budget] + "\n...[файл обрезан по бюджету]..."
                    blocks.append(block)
                    used += len(block)
                break
            blocks.append(block)
            used += len(block)

        return blocks, imgs

    def _render_planner_input(blocks: list[str]) -> str:
        parts = []
        if project_tree and project_tree.strip():
            parts.append(f"# Дерево проекта:\n{project_tree.strip()[:5000]}")
        if blocks:
            parts.append("# Содержимое файлов с сервера:\n\n" + "\n\n".join(blocks))
        else:
            parts.append("# Файлы на сервере:\nФайлов на сервере нет.")

        entries = _compress_history_entries(recent_history, char_budget=5000, max_items=6)
        if entries:
            h_lines = [f"[{'Пользователь' if r == 'user' else ('IDE' if r in ('tool', 'system') else 'Ассистент')}]: {t}" for r, t in entries]
            parts.append("# Недавний контекст:\n" + "\n\n".join(h_lines))

        parts.append(f"# Задача:\n{task_prompt}")
        return "\n\n".join(parts)

    is_25 = "2.5" in planner_model
    thinking_cfg = types.ThinkingConfig(thinking_budget=1024) if is_25 else types.ThinkingConfig(thinking_level="low")
    afc_cfg = types.AutomaticFunctionCallingConfig(disable=True) if hasattr(types, "AutomaticFunctionCallingConfig") else None
    cfg_kwargs = {
        "system_instruction": PLANNER_SYSTEM_INSTRUCTION,
        "max_output_tokens": 8192,
        "response_mime_type": "application/json",
        "thinking_config": thinking_cfg,
    }
    if afc_cfg is not None:
        cfg_kwargs["automatic_function_calling"] = afc_cfg
    gen_cfg = types.GenerateContentConfig(**cfg_kwargs)

    if status_callback:
        short_m = planner_model.replace("models/", "")
        await status_callback({"status": f"Планировщик ({short_m}) изучает файлы..."})

    # Одна «тяжёлая» попытка + до 2 повторов на другом ключе с урезанным контекстом.
    # 504 DEADLINE_EXCEEDED / 503 UNAVAILABLE — это перегрузка на стороне Google,
    # ключ при этом живой, поэтому винить его (register_key_error) нельзя.
    attempts = PLANNER_ATTEMPTS
    last_err = None
    for attempt in range(attempts):
        if attempt > 0:
            key_idx = key_pool.pick_key_for_request(model=planner_model, avoid_index=key_idx)

        # Каждый повтор собирается заново: меньше файлов, меньше картинок.
        files_budget = PLANNER_FILES_CHAR_BUDGET if attempt == 0 else PLANNER_RETRY_CHAR_BUDGET
        file_blocks, image_parts = _build_planner_messages(files_budget, shrink_images=attempt > 0)
        planner_input = _render_planner_input(file_blocks)
        contents = [types.Content(role="user", parts=[types.Part.from_text(text=planner_input), *image_parts])]

        logger.info(
            f"🧠 [PLANNER {planner_model}] попытка {attempt + 1}/{attempts} | Ключ #{key_idx} | "
            f"файлов={len(file_blocks)} | картинок={len(image_parts)} | символов входа={len(planner_input)}"
        )

        client = create_genai_client(api_keys, key_idx, timeout=PLANNER_HTTP_TIMEOUT_MS)
        try:
            resp = await run_generate_content_in_thread(
                client,
                timeout_sec=PLANNER_WAIT_TIMEOUT_SEC,
                model=planner_model,
                contents=contents,
                config=gen_cfg,
            )
            key_pool.mark_success(key_idx, model=planner_model)

            um = getattr(resp, "usage_metadata", None)
            u_in = int(getattr(um, "prompt_token_count", 0) or 0) if um else 0
            u_cac = int(getattr(um, "cached_content_token_count", 0) or 0) if um else 0
            u_out = int(getattr(um, "candidates_token_count", 0) or 0) if um else 0
            u_tot = int(getattr(um, "total_token_count", 0) or 0) if um else 0
            logger.info(f"📊 [PLANNER TOKENS] {planner_model} | in={u_in} cached={u_cac} out={u_out} total={u_tot}")

            raw_json = (getattr(resp, "text", None) or "").strip()
            raw_json = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw_json, flags=re.IGNORECASE | re.DOTALL).strip()
            plan_data = json.loads(raw_json)
            if not isinstance(plan_data, dict):
                raise ValueError("Planner JSON is not an object")

            return {
                "plan": plan_data,
                "assigned_key": key_idx,
                "usage": {"input": u_in, "cached": u_cac, "output": u_out, "total": u_tot},
            }
        except Exception as e:
            last_err = e
            transient = is_transient_error(e)
            if transient and attempt < attempts - 1:
                delay = PLANNER_RETRY_DELAYS[attempt] if attempt < len(PLANNER_RETRY_DELAYS) else 8.0
                logger.warning(
                    f"⚠️ [PLANNER RETRY {attempt + 1}/{attempts}] временная ошибка бэкенда "
                    f"({type(e).__name__}: {str(e)[:160]}) — повтор на другом ключе через {delay:.0f}с "
                    f"с урезанным контекстом ({PLANNER_RETRY_CHAR_BUDGET} симв.)..."
                )
                if status_callback:
                    await status_callback({"status": f"Планировщик перегружен, повтор через {delay:.0f}с..."})
                await asyncio.sleep(delay)
                continue
            logger.warning(f"⚠️ [PLANNER ERROR] {e}")
            # Ключ наказываем ТОЛЬКО за ошибки, которые действительно про ключ
            # (401/403/429). Перегрузка Google, таймауты и кривой JSON — не повод
            # сажать живой ключ в паузу на 15 минут или на сутки.
            if not transient and key_pool.classify_key_error(e):
                key_pool.register_key_error(key_idx, model=planner_model, exc=e)
            raise

    raise last_err


run_direct_hydrogen = run_direct