import traceback
import time
from datetime import datetime
from pathlib import Path
import argparse

import os
import base64
import json
from typing import Optional
from dotenv import load_dotenv
from supabase import Client, create_client


def _load_env() -> None:
    """Load .env next to this script (works even if cwd differs)."""
    env_path = Path(__file__).with_name('.env')
    load_dotenv(dotenv_path=env_path)


def _supabase_ref_from_url(url: str) -> Optional[str]:
    try:
        # https://<ref>.supabase.co
        host = url.split('://', 1)[1]
        return host.split('.', 1)[0]
    except Exception:
        return None


def _supabase_ref_from_jwt(jwt_token: str) -> Optional[str]:
    """Extract 'ref' from Supabase JWT (no signature verification)."""
    try:
        parts = jwt_token.split('.')
        if len(parts) < 2:
            return None
        payload_b64 = parts[1]
        payload_b64 += '=' * (-len(payload_b64) % 4)
        payload = json.loads(base64.urlsafe_b64decode(payload_b64.encode('utf-8')).decode('utf-8'))
        return payload.get('ref')
    except Exception:
        return None


_load_env()

SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_ANON_KEY')
SUPABASE_TABLE = os.getenv('SUPABASE_TABLE', 'a_dis')
POLL_MINUTES = int(os.getenv('POLL_MINUTES', '10'))
EXCEL_PATH = os.getenv('EXCEL_PATH', '监控列表.xlsx')
SHEET_NAME = os.getenv('SHEET_NAME', 'Sheet1')

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError(
        '缺少 Supabase 配置：方案B需要在 .env 中设置 SUPABASE_URL 和 SUPABASE_ANON_KEY（Publishable key），并在 Supabase 上启用 RLS + INSERT policy。'
    )

url_ref = _supabase_ref_from_url(SUPABASE_URL)
key_ref = _supabase_ref_from_jwt(SUPABASE_KEY)
if url_ref and key_ref and url_ref != key_ref:
    raise RuntimeError(
        "Supabase 配置不匹配：SUPABASE_URL 的项目 ref="
        f"{url_ref}，但 SUPABASE_ANON_KEY 的 ref={key_ref}。\n"
        "这会导致你写入/查看的是不同项目，从而看起来‘写不进去’。\n"
        "请确保 URL 和 Key 来自同一个 Supabase 项目（Settings → API）。"
    )

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

def insertdata(data):
    try:
        # 方案B（anon key + RLS）下更安全的做法：只允许 insert。
        # 若主键 id 已存在，会报错；这里捕获并跳过即可。
        # 注意：supabase-py 默认 returning=representation，会触发 SELECT 权限校验。
        # 若你只给 anon 授权 INSERT（insert-only），这里必须用 returning='minimal'。
        supabase.table(SUPABASE_TABLE).insert(data, returning='minimal').execute()
        log(f"Supabase 写入成功 id={data.get('id')}")
    except Exception as err:
        # 常见错误：表不存在 / schema 不对 / RLS 拒绝 / 主键重复
        err_code = getattr(err, 'code', None)
        err_message = getattr(err, 'message', None)
        err_code_str = str(err_code) if err_code is not None else None

        err_obj = getattr(err, 'args', None)
        err_payload = None
        if isinstance(err, dict):
            err_payload = err
        elif err_obj and len(err_obj) == 1 and isinstance(err_obj[0], dict):
            err_payload = err_obj[0]

        payload_code = err_payload.get('code') if isinstance(err_payload, dict) else None

        if err_code_str == '42P01' or payload_code == '42P01':
            log(
                f"Supabase 写入失败 id={data.get('id')}：表不存在或 schema 不对。"
                f"请确认已在云端创建表 {SUPABASE_TABLE}（可用本目录 supabase_a_dis.sql）。错误：{err_message or str(err)}"
            )
        elif err_code_str == '42501' or payload_code == '42501':
            perm_msg = (err_payload or {}).get('message') or err_message or str(err)
            log(
                f"Supabase 写入失败 id={data.get('id')}：表权限不足（{err_message or (err_payload or {}).get('message')}）。"
                "请在 Supabase SQL Editor 执行本目录的 supabase_a_dis.sql（启用 RLS + grant insert + insert policy），"
                "或确认 SUPABASE_URL / SUPABASE_ANON_KEY 对应同一个项目。"
            )
        else:
            log(f"Supabase 写入失败 id={data.get('id')}（可能重复主键或RLS拒绝）: {err}")


def load_targets_from_excel() -> list:
    import pandas as pd

    excel_file = Path(EXCEL_PATH)
    if not excel_file.exists():
        raise FileNotFoundError(f"未找到 Excel 文件：{EXCEL_PATH}（可在 .env 中设置 EXCEL_PATH）")
    df = pd.read_excel(str(excel_file), sheet_name=SHEET_NAME, engine='openpyxl')
    data_list = df.to_dict(orient='records')
    if not isinstance(data_list, list) or len(data_list) == 0:
        raise ValueError(f"Excel 读取到的目标列表为空：{EXCEL_PATH} / {SHEET_NAME}")
    return data_list


def job(driver, data_list: list) -> None:
    log(f"任务开始：targets={len(data_list)}")
    ok_count = 0
    fail_count = 0
    wrote_count = 0
    for i, item in enumerate(data_list, start=1):
        typename = item.get('typename')
        url = item.get('url')
        log(f"[{i}/{len(data_list)}] 打开: {typename} -> {url}")
        driver.listen.start('/messages')
        driver.get(url)
        time.sleep(4)
        try:
            t0 = time.time()
            video_resp = driver.listen.wait(timeout=10)
            dt = time.time() - t0
            resdata = video_resp.response.body
            if not isinstance(resdata, list):
                raise TypeError(f"/messages 响应 body 不是 list（实际={type(resdata).__name__}）")
            if len(resdata) == 0:
                raise ValueError("/messages 响应 body 为空 list")
            datalist = resdata
            log(f"捕获 /messages 响应：items={len(datalist)} wait={dt:.2f}s")
            itemobj = datalist[-1] or {}
            msglist = itemobj.get('embeds')
            if msglist and len(msglist)>0:
                for idx, msgitem in enumerate(msglist):
                    content = msgitem.get('description')
                    fields = msgitem.get('fields')
                    if fields:
                        for field in fields:
                            content = (content or '') + '\n' + (field.get('name') or '') + ':' + (field.get('value') or '')
                    author = itemobj.get('author') or {}
                    temp_dict = {
                        'typename': typename,
                        'username': author.get('username'),
                        'createtime': itemobj.get('timestamp'),
                        'content': content,
                        'url': msgitem.get('url'),
                        # 兼容 Supabase 主键约束：同一条消息多个 embed 时，用后缀区分
                        'id': f"{itemobj.get('id')}_{idx}",
                    }
                    insertdata(temp_dict)
                    wrote_count += 1
            else:
                author = itemobj.get('author') or {}
                temp_dict = {
                    'typename': typename,
                    'username': author.get('username'),
                    'createtime': itemobj.get('timestamp'),
                    'content': itemobj.get('content'),
                    'url': '',
                    'id': itemobj.get('id'),
                }
                insertdata(temp_dict)
                wrote_count += 1
            ok_count += 1
        except Exception as e:
            traceback.print_exc()
            log(f"获取 /messages 失败: {e}")
            fail_count += 1
    log(f"任务结束：ok={ok_count} fail={fail_count} wrote={wrote_count}")


def main() -> None:
    parser = argparse.ArgumentParser(description='监控 /messages 并写入 Supabase')
    parser.add_argument('--once', action='store_true', help='立即执行一次任务后退出（用于验证能否跑通）')
    parser.add_argument('--limit', type=int, default=0, help='只跑前 N 个 targets（0 表示全部）')
    parser.add_argument('--supabase-test', action='store_true', help='只做一次 Supabase 写入连通性测试后退出（不启动浏览器）')
    args = parser.parse_args()

    if args.supabase_test:
        test_id = f"healthcheck_{int(time.time())}"
        insertdata(
            {
                'typename': 'healthcheck',
                'username': 'local',
                'createtime': datetime.now().isoformat(),
                'content': 'supabase insert test',
                'url': '',
                'id': test_id,
            }
        )
        log(f"Supabase 测试结束：id={test_id}（可去表 {SUPABASE_TABLE} 查看）")
        return

    from DrissionPage import ChromiumPage

    driver = ChromiumPage()
    data_list = load_targets_from_excel()
    if args.limit and args.limit > 0:
        data_list = data_list[: args.limit]

    if args.once:
        job(driver, data_list)
        return

    import schedule

    schedule.every(POLL_MINUTES).minutes.do(job, driver=driver, data_list=data_list)
    log(f"启动完成：Excel={EXCEL_PATH} sheet={SHEET_NAME} targets={len(data_list)}")
    log(f"定时任务已启动：每{POLL_MINUTES}分钟执行一次（下一次运行取决于启动时刻）")
    log("按 Ctrl+C 停止")

    try:
        while True:
            schedule.run_pending()
            # 避免看起来“无输出”：每分钟打一条心跳
            if datetime.now().second == 0:
                log("心跳：等待下一轮任务...")
            time.sleep(1)  # 每秒检查一次
    except KeyboardInterrupt:
        log("定时任务已停止")


if __name__ == '__main__':
    main()



# conda activate bubblecrawl
# python 监控.py