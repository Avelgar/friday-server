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
from .key_pool import GeminiKeyPool, create_genai_client
from .model_config import get_models_config, to_api_model
from .prompts import DIRECT_SYSTEM_INSTRUCTION, PLANNER_SYSTEM_INSTRUCTION

logger = logging.getLogger("AI_Service")

DIRECT_HISTORY_CHAR_BUDGET = 12000
ANTIGRAVITY_HISTORY_CHAR_BUDGET = 5000
PLANNER_FILES_CHAR_BUDGET = 150000
UPLOADS_DIR = "/opt/friday/storage/uploads"
IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".webp", ".gif"}
CALL_PREFIXES = ("[CALL_PLANNER", "[CALL_ANTIGRAVITY", "[ESCALATE_ANTIGRAVITY")


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


def parse_antigravity_call(text: str) -> dict | None:
    s = (text or "").strip()
    if "[ESCALATE_ANTIGRAVITY]" in s:
        return {"task": None, "files": [], "network": False}

    m = re.search(r"\[CALL_(?:PLANNER|ANTIGRAVITY):\s*(.+?)\]", s, flags=re.DOTALL | re.IGNORECASE)
    if not m:
        return None

    body = m.group(1).strip()
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


def _is_dead_project_error(err_str: str) -> bool:
    return (
        "'quota_limit_value': '0'" in err_str
        or "ApiRequestsPerMinutePerProjectPerRegion" in err_str
        or "API_KEY_INVALID" in err_str
        or "401" in err_str
    )


def _could_be_call_tag(text_upper: str) -> bool:
    return any(p.startswith(text_upper) or text_upper.startswith(p) for p in CALL_PREFIXES)


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

    key_idx = assigned_key if (assigned_key is not None and key_pool.is_usable(assigned_key, model=model_name)) else key_pool.get_best_key(model=model_name)
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
                stripped_up = early_buf.lstrip().upper()
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
        err_str = str(e)
        logger.warning(f"⚠️ [DIRECT {model_name}] Ошибка на ключе #{key_idx}: {e}")
        if flushed_any:
            return {
                "escalate": False,
                "text": "".join(full_parts).strip(),
                "file_urls": {},
                "assigned_key": key_idx,
                "usage": usage_info,
            }
        if _is_dead_project_error(err_str):
            key_pool.mark_dead_all_models(key_idx)
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
) -> dict:
    """Читает файлы (и скриншоты) с диска и строит пошаговый план через модель Планировщика (250K TPM)."""
    cfg = get_models_config()
    planner_model = to_api_model(cfg["router_model"])
    key_idx = assigned_key if (assigned_key is not None and key_pool.is_usable(assigned_key, model=planner_model)) else key_pool.get_best_key(model=planner_model)

    selected_norm = {
        re.sub(r"^(\./)+", "", f.strip().replace("\\", "/")).lstrip("/")
        for f in (selected_files or [])
        if f and f.strip()
    }

    ordered_files = list(file_manifest or [])
    if selected_norm:
        ordered_files.sort(key=lambda f: (0 if f["path"] in selected_norm else 1, -f.get("mtime", 0)))

    file_blocks = []
    image_parts = []
    used_chars = 0

    for f in ordered_files:
        disk_p = _resolve_disk_path(f)
        if not disk_p:
            continue

        ext = os.path.splitext(f["path"])[1].lower()
        if ext in IMAGE_EXTS:
            if len(image_parts) < 3 and os.path.getsize(disk_p) <= 10 * 1024 * 1024:
                try:
                    with open(disk_p, "rb") as imgf:
                        img_bytes = imgf.read()
                    mime, _ = mimetypes.guess_type(disk_p)
                    image_parts.append(types.Part.from_bytes(data=img_bytes, mime_type=mime or "image/png"))
                    file_blocks.append(f"=== ИЗОБРАЖЕНИЕ: {f['path']} (передано в запрос как картинка) ===")
                except Exception:
                    pass
            continue

        numbered = _read_numbered_file_content(disk_p, f["path"])
        if not numbered:
            continue
        block = f"=== ФАЙЛ: {f['path']} ===\n{numbered}"
        if used_chars + len(block) > PLANNER_FILES_CHAR_BUDGET:
            if not file_blocks:
                block = block[:PLANNER_FILES_CHAR_BUDGET] + "\n...[файл обрезан по бюджету]..."
                file_blocks.append(block)
            break
        file_blocks.append(block)
        used_chars += len(block)

    parts = []
    if project_tree and project_tree.strip():
        parts.append(f"# Дерево проекта:\n{project_tree.strip()[:5000]}")
    if file_blocks:
        parts.append("# Содержимое файлов с сервера:\n\n" + "\n\n".join(file_blocks))
    else:
        parts.append("# Файлы на сервере:\nФайлов на сервере нет.")

    entries = _compress_history_entries(recent_history, char_budget=5000, max_items=6)
    if entries:
        h_lines = [f"[{'Пользователь' if r == 'user' else ('IDE' if r in ('tool', 'system') else 'Ассистент')}]: {t}" for r, t in entries]
        parts.append("# Недавний контекст:\n" + "\n\n".join(h_lines))

    parts.append(f"# Задача:\n{task_prompt}")
    planner_input = "\n\n".join(parts)

    content_parts = [types.Part.from_text(text=planner_input), *image_parts]
    contents = [types.Content(role="user", parts=content_parts)]

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

    logger.info(f"🧠 [PLANNER {planner_model}] Ключ #{key_idx} | файлов={len(file_blocks)} | картинок={len(image_parts)} | символов входа={len(planner_input)}")

    client = create_genai_client(api_keys, key_idx, timeout=60000)
    last_err = None
    for attempt in range(2):
        try:
            resp = await asyncio.wait_for(
                asyncio.to_thread(
                    client.models.generate_content,
                    model=planner_model,
                    contents=contents,
                    config=gen_cfg,
                ),
                timeout=65.0,
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
            err_s = str(e)
            if "503" in err_s and attempt == 0:
                logger.warning("⚠️ [PLANNER 503] Модель временно перегружена, повтор через 2.5с...")
                await asyncio.sleep(2.5)
                continue
            logger.warning(f"⚠️ [PLANNER ERROR] {e}")
            if _is_dead_project_error(err_s):
                key_pool.mark_dead_all_models(key_idx)
            raise

    raise last_err


run_direct_hydrogen = run_direct