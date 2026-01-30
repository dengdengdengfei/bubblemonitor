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


def insertdata(data: dict) -> None:
    # 用 upsert 避免因主键 id 重复导致插入失败
    try:
        resp = supabase.table(SUPABASE_TABLE).upsert(data, on_conflict="id").execute()
        resp_error = getattr(resp, "error", None)
        if resp_error:
            print(f"写入 Supabase 失败: {resp_error}")
            return
        print(f"写入 Supabase 成功: id={data.get('id')}")
    except Exception as err:
        print(f"写入 Supabase 失败: {err}")


def job(driver: ChromiumPage, data_list: list) -> None:
    print(f"任务执行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    for item in data_list:
        typename = item.get('typename')
        url = item.get('url')
        print(url)
        driver.listen.start('/messages')
        driver.get(url)
        time.sleep(4)
        try:
            video_resp = driver.listen.wait(timeout=10)
            if not video_resp or isinstance(video_resp, bool):
                print("未捕获到 /messages 响应（可能超时/未登录/无权限/页面未触发请求）")
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