from __future__ import annotations

import os
import time
import traceback
from datetime import datetime
from pathlib import Path

import pandas as pd
import schedule
from DrissionPage import ChromiumPage

try:
    from dotenv import find_dotenv, load_dotenv
except Exception as e:  # pragma: no cover
    raise RuntimeError(
        "缺少依赖 python-dotenv，请在 bubblecrawl 环境执行：\n"
        "  python -m pip install python-dotenv"
    ) from e

try:
    from supabase import create_client
except Exception as e:  # pragma: no cover
    raise RuntimeError(
        "缺少依赖 supabase，请在 bubblecrawl 环境执行：\n"
        "  python -m pip install supabase"
    ) from e


def _load_env_and_get_base_dir() -> Path:
    # 优先从当前工作目录向上查找（便于你在任意目录运行）
    dotenv_path = find_dotenv(filename=".env", usecwd=True)
    if not dotenv_path:
        # 兜底：从脚本所在目录向上查找（最常见：.env 和脚本放同目录）
        dotenv_path = find_dotenv(filename=".env", usecwd=False)

    if dotenv_path:
        load_dotenv(dotenv_path)
        return Path(dotenv_path).resolve().parent

    # 找不到 .env 就用脚本目录作为基准，但 Supabase 变量会在后面校验
    return Path(__file__).resolve().parent


BASE_DIR = _load_env_and_get_base_dir()


def _resolve_path(path_value: str) -> Path:
    p = Path(path_value)
    if p.is_absolute():
        return p
    return (BASE_DIR / p).resolve()


SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_ANON_KEY")
SUPABASE_TABLE = os.getenv("SUPABASE_TABLE", "a_dis")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError(
        "Supabase 环境变量缺失：请在 .env 中设置 SUPABASE_URL 和 SUPABASE_ANON_KEY（或 SUPABASE_SERVICE_ROLE_KEY）。"
    )

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


def _env_int(name: str, default: int) -> int:
    val = os.getenv(name)
    if val is None or str(val).strip() == "":
        return default
    try:
        return int(str(val).strip())
    except ValueError:
        return default


LISTEN_PATH = os.getenv("LISTEN_PATH", "/messages")
LISTEN_TIMEOUT = _env_int("LISTEN_TIMEOUT", 15)
PAGE_WAIT_SECONDS = _env_int("PAGE_WAIT_SECONDS", 4)
RETRIES = _env_int("RETRIES", 2)


def _pick_first(item: dict, keys: list[str]) -> str | None:
    for key in keys:
        val = item.get(key)
        if val is None:
            continue
        s = str(val).strip()
        if s:
            return s
    return None


def _get_item_url(item: dict) -> str | None:
    return _pick_first(item, ["url", "URL", "网址", "链接", "link", "Link"])


def _get_item_typename(item: dict) -> str:
    return _pick_first(item, ["typename", "type", "分类", "类别", "name", "名称"]) or ""


def insertdata(data: dict) -> None:
    # 使用 insert（只需要 INSERT 权限）。
    # 注意：upsert 会走 ON CONFLICT DO UPDATE，通常还需要 UPDATE 权限，
    # 如果你按“只放行 INSERT”的 RLS/GRANT 配置，会直接报 permission denied。
    try:
        resp = supabase.table(SUPABASE_TABLE).insert(data).execute()
        resp_error = getattr(resp, "error", None)
        if resp_error:
            code = None
            try:
                code = resp_error.get("code")
            except Exception:
                pass

            # 23505: unique_violation（主键重复）——忽略即可
            if code == "23505":
                print(f"已存在，跳过: id={data.get('id')}")
                return

            # 42501: insufficient_privilege
            if code == "42501":
                print(
                    "写入 Supabase 失败: permission denied。\n"
                    "请确认 Supabase 已执行 supabase_a_dis.sql（GRANT INSERT + RLS policy）。\n"
                    "或在 .env 改用 SUPABASE_SERVICE_ROLE_KEY（不推荐公开环境）。"
                )
                return

            print(f"写入 Supabase 失败: {resp_error}")
            return
        print(f"写入 Supabase 成功: id={data.get('id')}")
    except Exception as err:
        print(f"写入 Supabase 失败: {err}")


def job(driver: ChromiumPage, data_list: list) -> None:
    print(f"任务执行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    for item in data_list:
        typename = _get_item_typename(item)
        url = _get_item_url(item)
        if not url:
            print(f"跳过：Excel 行缺少 url 字段：{item}")
            continue
        print(url)
        try:
            # 部分版本支持 clear；有的话就清一下，避免拿到上一轮的包
            if hasattr(driver.listen, "clear"):
                driver.listen.clear()
        except Exception:
            pass

        driver.listen.start(LISTEN_PATH)
        driver.get(url)
        time.sleep(PAGE_WAIT_SECONDS)
        try:
            current_url = getattr(driver, "url", "")
            if current_url and isinstance(current_url, str) and "login" in current_url:
                print("疑似未登录 Discord：当前页面跳转到 login，请先在浏览器里登录一次再跑脚本")

            video_resp = None
            for attempt in range(1, max(RETRIES, 1) + 1):
                candidate = driver.listen.wait(timeout=LISTEN_TIMEOUT)
                if candidate and not isinstance(candidate, bool):
                    video_resp = candidate
                    break
                print(
                    f"未捕获到 {LISTEN_PATH} 响应（attempt {attempt}/{RETRIES}，timeout={LISTEN_TIMEOUT}s）"
                )
                time.sleep(1)

            if not video_resp:
                print(
                    "本轮无数据：可能页面未触发接口、监听路径不匹配、网络慢、或需要登录/权限。"
                )
                continue

            resdata = video_resp.response.body
            datalist = resdata
            if not isinstance(datalist, list) or len(datalist) == 0:
                print(f"/messages 响应不是列表或为空，type={type(datalist).__name__}")
                continue
            print(len(datalist))
            itemobj = datalist[-1]
            msglist = itemobj.get('embeds')
            if msglist and len(msglist)>0:
                for msgitem in msglist:
                    content = msgitem.get('description')
                    fields = msgitem.get('fields')
                    if fields:
                        for field in fields:
                            content = (content or '') + '\n' + (field.get('name') or '') + ':' + (field.get('value') or '')
                    temp_dict = {
                        'typename': typename,
                        'username': (itemobj.get('author') or {}).get('username'),
                        'createtime': itemobj.get('timestamp'),
                        'content': content,
                        'url': msgitem.get('url'),
                        'id': itemobj.get('id'),
                    }
                    insertdata(temp_dict)
            else:
                temp_dict = {
                    'typename': typename,
                    'username': (itemobj.get('author') or {}).get('username'),
                    'createtime': itemobj.get('timestamp'),
                    'content': itemobj.get('content'),
                    'url': '',
                    'id': itemobj.get('id'),
                }
                insertdata(temp_dict)
        except Exception as e:
            traceback.print_exc()
            print(f'获取视频信息失败: {e}')
    print("执行具体任务...")


def main() -> None:
    driver = ChromiumPage()

    if os.getenv("EXCEL_PATH"):
        excel_default = os.getenv("EXCEL_PATH")
    elif (BASE_DIR / "list.xlsx").exists():
        excel_default = "list.xlsx"
    elif (BASE_DIR / "监控列表.xlsx").exists():
        excel_default = "监控列表.xlsx"
    else:
        excel_default = "list.xlsx"

    excel_path = _resolve_path(excel_default)
    sheet_name = os.getenv("SHEET_NAME", "Sheet1")
    poll_minutes = int(os.getenv("POLL_MINUTES", "10"))
    run_on_start = os.getenv("RUN_ON_START", "1").strip().lower() not in {"0", "false", "no"}

    df = pd.read_excel(str(excel_path), sheet_name=sheet_name, engine='openpyxl')
    data_list = df.to_dict(orient='records')

    if run_on_start:
        job(driver=driver, data_list=data_list)

    schedule.every(poll_minutes).minutes.do(lambda: job(driver=driver, data_list=data_list))
    print(f"定时任务已启动，每{poll_minutes}分钟执行一次...")
    print("按 Ctrl+C 停止")

    try:
        while True:
            schedule.run_pending()
            time.sleep(1)  # 每秒检查一次
    except KeyboardInterrupt:
        print("\n定时任务已停止")


if __name__ == "__main__":
    main()



# conda activate bubblecrawl
# python 监控.py