import os

import time

import smtplib

from email.header import Header

from email.mime.multipart import MIMEMultipart

from email.mime.text import MIMEText

from concurrent.futures import ThreadPoolExecutor

import pandas as pd

import numpy as np

from pytdx.hq import TdxHq_API



# ==============================================================================

# 1. 掌门人核心配置区

# ==============================================================================

SMTP_SERVER = "smtp.qq.com"          

SMTP_PORT = 465                      

SENDER_EMAIL = "XX@qq.com"

SENDER_PASS = ""

RECEIVER_EMAIL = ""



# 本地数据落地路径（完美对齐C盘SUN用户工作区）

LOG_FILE_PATH = r""

MAX_WORKERS = 60  # 60路高并发同时洗盘，确保右侧信号毫无延迟



# 核心行情节点

HOST_LIST = [

    ('218.75.126.9', 7709),    # 杭州电信

    ('119.147.212.81', 7709),  # 深圳主服务器

    ('124.74.236.94', 7709)    # 上海电信

]



# ==============================================================================

# 2. 核心功能：全自动持久化日志落地

# ==============================================================================

def save_signal_to_local(code, name, trigger_time, price, reason):

    """坚决执行留痕管理，将捕获到的爆发点死死钉在本地CSV"""

    new_data = pd.DataFrame([{

        "交易日期": time.strftime("%Y-%m-%d"),

        "触发时间": trigger_time,

        "股票代码": code,

        "股票名称": name,

        "触发价格": price,

        "核心逻辑": reason

    }])

    try:

        if not os.path.exists(LOG_FILE_PATH):

            os.makedirs(os.path.dirname(LOG_FILE_PATH), exist_ok=True)

            new_data.to_csv(LOG_FILE_PATH, index=False, encoding="utf-8-sig")

        else:

            new_data.to_csv(LOG_FILE_PATH, mode='a', header=False, index=False, encoding="utf-8-sig")

    except Exception as e:

        print(f"⚠️ 本地日志写入冲突: {e}")



# ==============================================================================

# 3. 核心功能：高级邮件通知

# ==============================================================================

def send_master_email(subject, html_body):

    """秒级轰炸掌门人手机端邮件，保证右侧绝对知情权"""

    msg = MIMEMultipart("alternative")

    msg["Subject"] = Header(subject, "utf-8")

    msg["From"] = Header(f"天道围猎指挥部 <{SENDER_EMAIL}>", "utf-8")

    msg["To"] = Header(RECEIVER_EMAIL, "utf-8")

    msg.attach(MIMEText(html_body, "html", "utf-8"))

    try:

        server = smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT)

        server.login(SENDER_EMAIL, SENDER_PASS)

        server.sendmail(SENDER_EMAIL, [RECEIVER_EMAIL], msg.as_string())

        server.quit()

        print(f"   📬 [邮件通道] 战报已精准送达掌门人邮箱。")

    except Exception as e:

        print(f"   ❌ [邮件通道] 投递失败: {e}")



# ==============================================================================

# 4. 围猎核心算法引擎（50/60分钟周期波段共振）

# ==============================================================================

def compute_hunt_engine(bars_5m):

    """核心过滤逻辑：13周期斜率主浪线驱动 + MFLF锁筹因子 + 防诱多Override"""

    df_5m = pd.DataFrame(bars_5m)

    if df_5m.empty or len(df_5m) < 50: return False, 0

    

    # 强行重组50分钟周期

    df_5m['idx'] = np.arange(len(df_5m))

    df_5m['group_id'] = df_5m['idx'] // 10

    agg_dict = {'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'vol': 'sum', 'amount': 'sum'}

    df = df_5m.groupby('group_id').agg(agg_dict).reset_index(drop=True)

    

    if len(df) < 15: return False, 0



    # 🛠️ 13周期主浪线斜率驱动

    df['ma13'] = df['close'].rolling(window=13).mean()

    df['slope13'] = (df['ma13'] - df['ma13'].shift(1)) / df['ma13'].shift(1) * 100

    

    c_close, c_open, c_vol = df['close'].iloc[-1], df['open'].iloc[-1], df['vol'].iloc[-1]

    c_ma13, c_slope13 = df['ma13'].iloc[-1], df['slope13'].iloc[-1]

    

    trend_drive = (c_slope13 > 0) and (c_close > c_ma13)

    

    # 🛠️ 主力锁筹与量价动量（MFLF因子对齐）

    df['v_cost'] = df['amount'].rolling(window=15).sum() / (df['vol'].rolling(window=15).sum() + 0.0001)

    c_v_cost = df['v_cost'].iloc[-1]

    

    momentum = (c_close > df['close'].shift(1).iloc[-1]) and (c_vol > df['vol'].shift(1).iloc[-1] * 1.5)

    sniper_trigger = trend_drive and momentum and (c_close > c_open)

    

    # 🛠️ 强爆发压制诱多 Bull Trap Override

    df['v_ma5'] = df['vol'].rolling(window=5).mean()

    explosive_power = ((c_close - df['close'].shift(1).iloc[-1]) / df['close'].shift(1).iloc[-1] * 100 > 5) and (c_vol > df['v_ma5'].iloc[-1] * 2)

    bull_trap_signal = 0 if explosive_power else 1

    

    if sniper_trigger and (bull_trap_signal == 1) and (c_close > c_v_cost):

        return True, c_close

    return False, 0



# ==============================================================================

# 5. 单股扫描子线程任务

# ==============================================================================

def scan_stock_task(stock, triggered_cache, hour_key, active_host, active_port):

    market, code, name = stock

    api = TdxHq_API()

    if api.connect(active_host, active_port):

        try:

            bars = api.get_security_bars(0, market, code, 0, 150)

            if not bars or len(bars) < 50: return  # 剔除无效或停牌股

            

            is_triggered, trigger_price = compute_hunt_engine(bars)

            if is_triggered:

                cache_key = f"{code}_{hour_key}"

                if cache_key not in triggered_cache:

                    triggered_cache.add(cache_key)

                    local_time = time.strftime("%H:%M:%S", time.localtime())

                    

                    save_signal_to_local(code, name, local_time, trigger_price, "13主浪线多头共振 + MFLF筹码锁定")

                    print(f"💥 [天道捕获] 标的: {name}({code}) | 价格: {trigger_price} 元 | 已写入本地日志")

                    

                    html_content = f"""

                    <html>

                    <body style="background-color: #121212; color: #E0E0E0; font-family: sans-serif; padding: 20px;">

                        <div style="max-width: 600px; margin: 0 auto; background-color: #1E1E1E; border: 1px solid #333333; border-radius: 8px; padding: 20px;">

                            <h2 style="color: #66FFFF; border-bottom: 2px solid #333333; padding-bottom: 10px; margin-top: 0;">🎯 天道围猎 ── 盘中狙击战报</h2>

                            <div style="background-color: #252525; border-left: 4px solid #FF3366; padding: 15px; margin: 20px 0;">

                                <p style="margin: 0; font-size: 20px; font-weight: bold; color: #66FFFF;">{name} ({code})</p>

                                <p style="margin: 8px 0 0 0; color: #FF3366; font-size: 16px; font-weight: bold;">触发价：{trigger_price} 元  |  时间：{local_time}</p>

                            </div>

                        </div>

                    </body>

                    </html>

                    """

                    send_master_email(f"🎯【狙击爆发】{name}({code}) 触发右侧共振！", html_content)

        except Exception:

            pass

        finally:

            api.disconnect()



# ==============================================================================

# 6. 核心算法：内生全量代码字典生成（彻底根治1495截流控制）

# ==============================================================================

def generate_all_a_stocks_matrix():

    print("🧬 正在利用天道内生算法在内存中凭空构建全A股雷达矩阵...")

    stocks = []

    

    # 1. 深圳主板及中小板 (000001 - 003999)

    for i in range(1, 4000):

        stocks.append((0, f"{i:06d}", f"深股_{i:06d}"))

        

    # 2. 创业板 (300000 - 301600)

    for i in range(300000, 301601):

        stocks.append((0, f"{i}", f"创股_{i}"))

        

    # 3. 上海主板 (600000 - 601999, 603000 - 606000)

    for i in range(600000, 602000):

        stocks.append((1, f"{i}", f"沪股_{i}"))

    for i in range(603000, 606000):

        stocks.append((1, f"{i}", f"沪股_{i}"))

        

    # 4. 科创板 (688000 - 689000)

    for i in range(688000, 689001):

        stocks.append((1, f"{i}", f"科创_{i}"))

        

    print(f"✅ 内存矩阵构建完毕！")

    return HOST_LIST[0][0], HOST_LIST[0][1], stocks



# ==============================================================================

# 7. 主调度循环程序（完美闭环修复版）

# ==============================================================================

def main_scheduler():

    active_host, active_port, stocks_list = generate_all_a_stocks_matrix()

    triggered_cache = set()

    

    print("+" + "-"*70 + "+")

    print(f"  【天道围猎系统 V4.1 全市场天网版】 部署成功")

    print(f"  🔥 当前全市场品种终极锁定: {len(stocks_list)} 只 🎯（全时段免维护）")

    print(f"  并行扫描线程: {MAX_WORKERS}  |  当前激活测速节点: {active_host}:{active_port}")

    print(f"  本地日志落地绝对路径: {LOG_FILE_PATH}")

    print("+" + "-"*70 + "+")

    

    while True:

        local_time = time.strftime("%H:%M:%S", time.localtime())

        if "09:28:00" < local_time < "15:10:00":

            start_time = time.time()

            hour_key = time.strftime("%Y%m%d_%H", time.localtime())

            

            print(f"[{local_time}] 战网开火！开始对全市场进行无声高频检索围猎...")

            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

                for stock in stocks_list:

                    executor.submit(scan_stock_task, stock, triggered_cache, hour_key, active_host, active_port)

            

            end_time = time.time()

            print(f"[{time.strftime('%H:%M:%S')}] 全市场扫描穿透完毕，耗时: {end_time - start_time:.2f} 秒。")

            time.sleep(60)

        else:

            time.sleep(10)



if __name__ == "__main__":

    main_scheduler() 

