import time
import datetime
import threading
import json
import pandas as pd
import numpy as np
import requests
from io import BytesIO
from binance.client import Client
import websocket
import pytz
import math
from prometheus_client import start_http_server, Gauge, Counter

# ====================================================
# Konfigurasi Prometheus Metrics
# ====================================================
active_orders_gauge = Gauge('active_orders', 'Number of active orders')
order_latency_gauge = Gauge('order_latency_seconds', 'Latency for order execution in seconds')
order_retry_counter = Counter('order_retry_total', 'Total number of order retries')
current_pnl_gauge = Gauge('current_pnl', 'Current profit and loss')
current_drawdown_gauge = Gauge('current_drawdown', 'Current drawdown')

# Mulai HTTP server untuk metrics (misal port 8000)
start_http_server(8000)

# ====================================================
# Set timezone Jakarta (GMT+7)
# ====================================================
jakarta_tz = pytz.timezone("Asia/Jakarta")

# ====================================================
# Konfigurasi API Binance Futures Testnet dan Discord Webhook
# ====================================================
api_key = "YOUR KEY"
api_secret = "YOUR SECRET"
client = Client(api_key, api_secret, testnet=True)

discord_webhook_url = "your webhook"

# ====================================================
# Fungsi untuk mengirim pesan ke Discord
# ====================================================
def send_discord_message(content, webhook_url=discord_webhook_url):
    data = {"content": content}
    try:
        response = requests.post(webhook_url, json=data)
        if response.status_code not in (200, 204):
            print("Failed to send Discord message:", response.text)
    except Exception as e:
        print("Error sending Discord message:", e)

# ====================================================
# Fungsi untuk mengambil data historis dari Binance Futures (REST API)
# ====================================================
def get_historical_klines(symbol, interval, start_str, end_str=None, limit=1000):
    start_ts = int(pd.Timestamp(start_str).timestamp() * 1000)
    end_ts = int(pd.Timestamp(end_str).timestamp() * 1000) if end_str else None
    klines = []
    
    while True:
        temp_klines = client.futures_klines(
            symbol=symbol,
            interval=interval,
            startTime=start_ts,
            endTime=end_ts,
            limit=limit
        )
        if not temp_klines:
            break
        klines.extend(temp_klines)
        if len(temp_klines) < limit:
            break
        start_ts = temp_klines[-1][0] + 1
    return klines

def prepare_data(symbol, interval, start_date, end_date):
    klines = get_historical_klines(symbol, interval, start_date, end_date)
    columns = ["open_time", "open", "high", "low", "close", "volume",
               "close_time", "quote_asset_volume", "number_of_trades",
               "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume", "ignore"]
    df = pd.DataFrame(klines, columns=columns)
    df["datetime"] = pd.to_datetime(df["open_time"], unit="ms").dt.tz_localize("UTC").dt.tz_convert(jakarta_tz)
    df[["open", "high", "low", "close"]] = df[["open", "high", "low", "close"]].astype(float)
    return df[["datetime", "open", "high", "low", "close"]]

def calculate_atr(data, period=14):
    data = data.copy()
    data['high-low'] = data['high'] - data['low']
    data['high-close'] = abs(data['high'] - data['close'].shift(1))
    data['low-close'] = abs(data['low'] - data['close'].shift(1))
    data['tr'] = data[['high-low', 'high-close', 'low-close']].max(axis=1)
    data['atr'] = data['tr'].rolling(window=period).mean()
    return data

# ====================================================
# Global Variabel dan Lock
# ====================================================
global_balance = 50.0         
initial_capital = 50.0        
leverage = 10
fee_rate = 0.0004             # 0.04%
slippage_rate = 0.0005        # 0.05%

# Daftar pair (misal BTCUSDT agar terlihat di Testnet)
pairs = ["BTCUSDT"]
historical_data = {}
current_prices = {}
open_trades = []      # Menyimpan detail posisi/order terbuka (simulasi order)
executed_trades = []  # Menyimpan detail order yang telah ditutup

data_lock = threading.Lock()

# ====================================================
# Fungsi untuk menghitung effective margin (free margin + floating PnL dari posisi terbuka)
# ====================================================
def get_effective_margin():
    effective = global_balance
    for trade in open_trades:
        symbol = trade['symbol']
        if symbol in current_prices:
            current_price = current_prices[symbol]
            if trade['direction'] == "long":
                floating = (current_price - trade['entry_price']) * trade['quantity'] * leverage
            else:
                floating = (trade['entry_price'] - current_price) * trade['quantity'] * leverage
            effective += floating
    return effective

# ====================================================
# Fungsi untuk menempatkan order dengan retry mechanism
# ====================================================
def place_orders(symbol, direction, entry_price, stop_loss, take_profit, quantity, max_retries=3):
    attempt = 0
    while attempt < max_retries:
        try:
            start_time = time.time()
            side = "BUY" if direction == "long" else "SELL"
            market_order = client.futures_create_order(
                symbol=symbol,
                side=side,
                type="MARKET",
                quantity=round(quantity, 6)
            )
            # Atur order stop loss dan take profit
            if direction == "long":
                stop_side = "SELL"
                tp_side = "SELL"
            else:
                stop_side = "BUY"
                tp_side = "BUY"
            stop_order = client.futures_create_order(
                symbol=symbol,
                side=stop_side,
                type="STOP_MARKET",
                stopPrice=round(stop_loss, 2),
                closePosition=True
            )
            tp_order = client.futures_create_order(
                symbol=symbol,
                side=tp_side,
                type="TAKE_PROFIT_MARKET",
                stopPrice=round(take_profit, 2),
                closePosition=True
            )
            latency = time.time() - start_time
            order_latency_gauge.set(latency)
            return {
                "market_order": market_order,
                "stop_order": stop_order,
                "tp_order": tp_order
            }
        except Exception as e:
            order_retry_counter.inc()
            print(f"[{symbol}] Order attempt {attempt+1} failed: {e}")
            time.sleep(1)
            attempt += 1
    send_discord_message(f"Failed to place orders for {symbol} after {max_retries} attempts.")
    return None

# ====================================================
# Fungsi untuk polling candle dan mendeteksi sinyal entry
# ====================================================
def poll_candles():
    global historical_data, open_trades

    interval = Client.KLINE_INTERVAL_15MINUTE

    init_start = (datetime.datetime.now(jakarta_tz) - datetime.timedelta(minutes=50 * 15)).strftime('%Y-%m-%d %H:%M:%S')
    init_end = datetime.datetime.now(jakarta_tz).strftime('%Y-%m-%d %H:%M:%S')
    for symbol in pairs:
        df = prepare_data(symbol, interval, init_start, init_end)
        df = calculate_atr(df)
        historical_data[symbol] = df

    send_discord_message("Inisialisasi data historis selesai. Mulai polling candle...")
    print("Inisialisasi data historis selesai. Mulai polling candle...")

    while True:
        try:
            for symbol in pairs:
                # Cegah order duplikasi: jika sudah ada order aktif/pending untuk simbol ini, lewati.
                with data_lock:
                    if any(trade for trade in open_trades if trade['symbol'] == symbol):
                        continue

                klines = client.futures_klines(symbol=symbol, interval=interval, limit=2)
                if not klines:
                    continue
                candle_open_time = pd.to_datetime(klines[-2][0], unit='ms').tz_localize("UTC").tz_convert(jakarta_tz)
                df = historical_data.get(symbol, pd.DataFrame())
                if not df.empty and candle_open_time <= df.iloc[-1]['datetime']:
                    continue
                new_row = {
                    'datetime': candle_open_time,
                    'open': float(klines[-2][1]),
                    'high': float(klines[-2][2]),
                    'low': float(klines[-2][3]),
                    'close': float(klines[-2][4])
                }
                new_df = pd.DataFrame([new_row])
                df = pd.concat([df, new_df], ignore_index=True)
                df = calculate_atr(df)
                historical_data[symbol] = df

                if len(df) < 15:
                    continue

                candidate = df.iloc[-2]
                current = df.iloc[-1]
                atr = candidate.get('atr', np.nan)
                if np.isnan(atr):
                    continue

                long_signal = current['close'] > candidate['high']
                short_signal = current['close'] < candidate['low']
                if not (long_signal or short_signal):
                    continue

                with data_lock:
                    # Pastikan kembali tidak ada posisi aktif untuk simbol ini (pencegahan duplikasi)
                    if any(trade for trade in open_trades if trade['symbol'] == symbol):
                        continue

                    entry_time = current['datetime']
                    entry_price = current['open']
                    if long_signal:
                        direction = "long"
                        stop_loss = entry_price - (1.5 * atr)
                        take_profit = entry_price + (2.0 * atr)
                    else:
                        direction = "short"
                        stop_loss = entry_price + (1.5 * atr)
                        take_profit = entry_price - (2.0 * atr)
                    
                    # Dynamic Position Sizing dengan volatility-adjusted sizing:
                    # Risk percent dasar (misalnya 5%) dikurangi berdasarkan volatilitas relatif.
                    volatility_scale = 10.0
                    # Semakin tinggi ATR relatif terhadap entry_price, semakin kecil risk percent yang digunakan.
                    dynamic_risk_percent = 0.05 / (1 + volatility_scale * (atr / entry_price))
                    effective_margin = get_effective_margin()
                    risk_amount = dynamic_risk_percent * effective_margin
                    risk_distance = abs(entry_price - stop_loss)
                    if risk_distance == 0:
                        continue
                    quantity = risk_amount / risk_distance
                    quantity = max(quantity, 0.001)
                    
                    orders = place_orders(symbol, direction, entry_price, stop_loss, take_profit, quantity)
                    if orders is None:
                        continue

                    trade = {
                        'entry_time': entry_time,
                        'symbol': symbol,
                        'direction': direction,
                        'entry_price': entry_price,
                        'stop_loss': stop_loss,
                        'take_profit': take_profit,
                        'quantity': quantity,
                        'orders': orders,
                        'status': 'open'
                    }
                    open_trades.append(trade)
                    active_orders_gauge.set(len(open_trades))
                    msg = (f"🚀 **Trade OPENED** untuk **{symbol}** pada {entry_time.strftime('%Y-%m-%d %H:%M:%S %Z')}\n"
                           f"🔀 Direction   : {direction}\n"
                           f"📈 Entry Price : {entry_price:.4f}\n"
                           f"🛑 Stop Loss   : {stop_loss:.4f}\n"
                           f"🎯 Take Profit : {take_profit:.4f}\n"
                           f"📦 Quantity    : {quantity:.4f}")
                    send_discord_message(msg)
                    print(msg)
            time.sleep(10)
        except Exception as e:
            print("Error in poll_candles:", e)
            time.sleep(5)

# ====================================================
# Fungsi WebSocket untuk menerima tick data real-time (digunakan untuk trailing stop)
# ====================================================
def on_message(ws, message):
    global current_prices, open_trades
    try:
        data = json.loads(message)
        trade_data = data.get("data", {})
        symbol = trade_data.get("s", "")
        price = float(trade_data.get("p", 0))
        current_prices[symbol] = price

        with data_lock:
            for trade in open_trades.copy():
                if trade['symbol'] != symbol:
                    continue

                # Mekanisme trailing stop:
                # Untuk posisi long: jika profit mencapai ≥30% (price ≥ 1.30×entry),
                # batalkan order SL lama dan buat order SL baru di level break even (entry price).
                if trade['direction'] == "long" and price >= trade['entry_price'] * 1.30:
                    try:
                        client.futures_cancel_order(symbol=symbol, orderId=trade['orders']['stop_order']['orderId'])
                        new_stop = client.futures_create_order(
                            symbol=symbol,
                            side="SELL",
                            type="STOP_MARKET",
                            stopPrice=round(trade['entry_price'], 2),
                            closePosition=True
                        )
                        trade['orders']['stop_order'] = new_stop
                        send_discord_message(f"🔄 Update SL untuk {symbol} (LONG): SL digeser ke BEP = {trade['entry_price']:.4f} USDT")
                    except Exception as e:
                        print(f"Error updating SL untuk {symbol}: {e}")
                # Untuk posisi short: jika profit mencapai ≥30% (price ≤ 0.70×entry), geser SL ke BEP.
                elif trade['direction'] == "short" and price <= trade['entry_price'] * 0.70:
                    try:
                        client.futures_cancel_order(symbol=symbol, orderId=trade['orders']['stop_order']['orderId'])
                        new_stop = client.futures_create_order(
                            symbol=symbol,
                            side="BUY",
                            type="STOP_MARKET",
                            stopPrice=round(trade['entry_price'], 2),
                            closePosition=True
                        )
                        trade['orders']['stop_order'] = new_stop
                        send_discord_message(f"🔄 Update SL untuk {symbol} (SHORT): SL digeser ke BEP = {trade['entry_price']:.4f} USDT")
                    except Exception as e:
                        print(f"Error updating SL untuk {symbol}: {e}")
    except Exception as e:
        print("Error in on_message:", e)

def on_error(ws, error):
    print("WebSocket error:", error)

def on_close(ws, close_status_code, close_msg):
    print("WebSocket closed")

def on_open(ws):
    print("WebSocket connection opened.")

def start_websocket():
    streams = [f"{symbol.lower()}@trade" for symbol in pairs]
    stream_url = "wss://stream.binancefuture.com/stream?streams=" + "/".join(streams)
    ws = websocket.WebSocketApp(
        stream_url,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    ws.on_open = on_open
    ws.run_forever()

# ====================================================
# (Opsional) Fungsi Summary: Daily, Weekly, Monthly
# ====================================================
def daily_summary():
    global executed_trades, global_balance
    bot_start_time = datetime.datetime.now(jakarta_tz)
    last_summary_time = datetime.datetime.now(jakarta_tz)
    while True:
        now = datetime.datetime.now(jakarta_tz)
        if (now - last_summary_time).total_seconds() >= 24 * 3600:
            with data_lock:
                times = [trade['exit_time'] for trade in executed_trades if 'exit_time' in trade]
                balances = [trade['balance_after'] for trade in executed_trades if 'balance_after' in trade]
            total_trades = len(executed_trades)
            win_count = sum(1 for t in executed_trades if t.get('pnl', 0) > 0)
            loss_count = total_trades - win_count
            win_rate = (win_count / total_trades * 100) if total_trades > 0 else 0
            summary_msg = (f"📊 **Daily Summary** sejak {bot_start_time.strftime('%Y-%m-%d %H:%M:%S %Z')}:\n"
                           f"💵 Final Balance: ${global_balance:.2f}\n"
                           f"📈 Total Trades: {total_trades}\n"
                           f"✅ Wins: {win_count}, ❌ Losses: {loss_count}\n"
                           f"🏆 Win Rate: {win_rate:.2f}%")
            send_discord_message(summary_msg)
            print("Daily summary sent.")
            last_summary_time = now
        time.sleep(60)

def weekly_summary():
    global executed_trades, global_balance
    last_summary_time = datetime.datetime.now(jakarta_tz)
    while True:
        now = datetime.datetime.now(jakarta_tz)
        if (now - last_summary_time).total_seconds() >= 7 * 24 * 3600:
            start_period = now - datetime.timedelta(days=7)
            trades_week = [trade for trade in executed_trades if 'exit_time' in trade and trade['exit_time'] >= start_period]
            total_trades = len(trades_week)
            win_count = sum(1 for t in trades_week if t.get('pnl', 0) > 0)
            loss_count = total_trades - win_count
            win_rate = (win_count / total_trades * 100) if total_trades > 0 else 0
            summary_msg = (f"📆 **Weekly Summary** ({start_period.strftime('%Y-%m-%d')} - {now.strftime('%Y-%m-%d')}):\n"
                           f"💵 Balance: ${global_balance:.2f}\n"
                           f"📈 Total Trades: {total_trades}\n"
                           f"✅ Wins: {win_count}, ❌ Losses: {loss_count}\n"
                           f"🏆 Win Rate: {win_rate:.2f}%")
            send_discord_message(summary_msg)
            print("Weekly summary sent.")
            last_summary_time = now
        time.sleep(60)

def monthly_summary():
    global executed_trades, global_balance
    last_summary_time = datetime.datetime.now(jakarta_tz)
    while True:
        now = datetime.datetime.now(jakarta_tz)
        if (now - last_summary_time).total_seconds() >= 30 * 24 * 3600:
            start_period = now - datetime.timedelta(days=30)
            trades_month = [trade for trade in executed_trades if 'exit_time' in trade and trade['exit_time'] >= start_period]
            total_trades = len(trades_month)
            win_count = sum(1 for t in trades_month if t.get('pnl', 0) > 0)
            loss_count = total_trades - win_count
            win_rate = (win_count / total_trades * 100) if total_trades > 0 else 0
            summary_msg = (f"📅 **Monthly Summary** ({start_period.strftime('%Y-%m-%d')} - {now.strftime('%Y-%m-%d')}):\n"
                           f"💵 Balance: ${global_balance:.2f}\n"
                           f"📈 Total Trades: {total_trades}\n"
                           f"✅ Wins: {win_count}, ❌ Losses: {loss_count}\n"
                           f"🏆 Win Rate: {win_rate:.2f}%")
            send_discord_message(summary_msg)
            print("Monthly summary sent.")
            last_summary_time = now
        time.sleep(60)

# ====================================================
# Fungsi untuk update global balance
# ====================================================
def update_global_balance():
    global global_balance, initial_capital
    # For simulation, kita set ulang global_balance ke initial_capital
    global_balance = initial_capital

# ====================================================
# Fungsi utama
# ====================================================
def main():
    start_msg = (f"🤖 Bot STARTED pada {datetime.datetime.now(jakarta_tz).strftime('%Y-%m-%d %H:%M:%S %Z')}\n"
                 f"💱 Pairs: {', '.join(pairs)}\n"
                 f"🔢 Leverage: {leverage}x")
    send_discord_message(start_msg)
    print(start_msg)

    update_global_balance()  # Inisialisasi saldo

    # Jalankan thread polling candle, websocket, dan summary
    candle_thread = threading.Thread(target=poll_candles, daemon=True)
    candle_thread.start()

    ws_thread = threading.Thread(target=start_websocket, daemon=True)
    ws_thread.start()

    daily_thread = threading.Thread(target=daily_summary, daemon=True)
    daily_thread.start()
    weekly_thread = threading.Thread(target=weekly_summary, daemon=True)
    weekly_thread.start()
    monthly_thread = threading.Thread(target=monthly_summary, daemon=True)
    monthly_thread.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Bot dihentikan oleh user.")

if __name__ == "__main__":
    main()
