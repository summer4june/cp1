import os
import sys
import json
import sqlite3
import subprocess
import threading
from datetime import datetime, timezone
from flask import Flask, jsonify, request, render_template_string

app = Flask(__name__)

# Paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, "config.json")
DB_PATH = os.path.join(BASE_DIR, "db", "fxbot.db")
BACKTEST_RESULTS_DIR = os.path.join(BASE_DIR, "backtest", "results")

# State for background backtesting
backtest_process = None
backtest_output = []
backtest_status = "idle"  # idle | running | completed | failed

def run_backtest_thread():
    global backtest_process, backtest_output, backtest_status
    backtest_status = "running"
    backtest_output = ["🚀 Starting Backtest Subprocess...\n"]
    
    try:
        # Run python3 backtest.py in the background
        backtest_process = subprocess.Popen(
            [sys.executable, "backtest.py"],
            cwd=BASE_DIR,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1
        )
        
        for line in iter(backtest_process.stdout.readline, ""):
            backtest_output.append(line)
            # Cap output to avoid huge memory logs
            if len(backtest_output) > 2000:
                backtest_output.pop(1)
                
        backtest_process.stdout.close()
        return_code = backtest_process.wait()
        
        if return_code == 0:
            backtest_status = "completed"
            backtest_output.append("\n✅ Backtest Completed Successfully!")
        else:
            backtest_status = "failed"
            backtest_output.append(f"\n❌ Backtest Failed with Exit Code: {return_code}")
            
    except Exception as e:
        backtest_status = "failed"
        backtest_output.append(f"\n❌ Subprocess Execution Error: {e}")

@app.route("/")
def home():
    return render_template_string(HTML_TEMPLATE)

@app.route("/api/config", methods=["GET"])
def get_config():
    if not os.path.exists(CONFIG_PATH):
        return jsonify({"error": "config.json not found"}), 404
        
    with open(CONFIG_PATH, "r") as f:
        config_data = json.load(f)
    return jsonify(config_data)

@app.route("/api/config", methods=["POST"])
def update_config():
    if not os.path.exists(CONFIG_PATH):
        return jsonify({"error": "config.json not found"}), 404
        
    try:
        new_config = request.json
        # Maintain some validation structure compatibility
        with open(CONFIG_PATH, "w") as f:
            json.dump(new_config, f, indent=2)
        return jsonify({"success": True, "message": "Configuration saved successfully!"})
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route("/api/trades", methods=["GET"])
def get_trades():
    if not os.path.exists(DB_PATH):
        return jsonify({"executed": [], "skipped": [], "error": f"Database file not found at {DB_PATH}. Run bot or tests first."})
        
    executed = []
    skipped = []
    
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        
        # Executed Trades
        cursor = conn.execute("SELECT * FROM trades_executed ORDER BY execution_time DESC LIMIT 100")
        executed = [dict(row) for row in cursor.fetchall()]
        
        # Skipped/Denied Trades
        cursor = conn.execute("SELECT * FROM trades_skipped ORDER BY skip_time DESC LIMIT 100")
        skipped = [dict(row) for row in cursor.fetchall()]
        
        conn.close()
    except Exception as e:
        return jsonify({"error": f"Database query failed: {e}"}), 500
        
    return jsonify({"executed": executed, "skipped": skipped})

@app.route("/api/live_trades", methods=["GET"])
def get_live_trades():
    if not os.path.exists(DB_PATH):
        return jsonify([])
        
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.execute("SELECT * FROM trades_executed WHERE status = 'OPEN' ORDER BY execution_time DESC")
        live = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return jsonify(live)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/scan", methods=["POST"])
def trigger_scan():
    # Execute a manual non-blocking scan loop on configured pairs
    try:
        # Avoid circular dependencies, import engines dynamically
        from core.configengine import ConfigEngine
        from core.stateengine import StateEngine
        from core.mt5connector import MT5Connector
        from modules.sessionengine import SessionEngine
        from modules.riskengine import RiskEngine
        from modules.correlationfilter import CorrelationFilter
        from modules.scannermmxm import ScannerMMXM
        from modules.scannerote import ScannerOTE
        
        config = ConfigEngine().get_config()
        state_engine = StateEngine(DB_PATH)
        mt5_connector = MT5Connector(config)
        session_engine = SessionEngine(config)
        risk_engine = RiskEngine(config, mt5_connector)
        correlation_filter = CorrelationFilter(config)
        
        # Initialize scanners based on config
        scanners = []
        if config.enabled_scanners.get("mmxm", True):
            scanners.append(("ScannerMMXM", ScannerMMXM(config, mt5_connector, state_engine)))
        if config.enabled_scanners.get("ote", False):
            scanners.append(("ScannerOTE", ScannerOTE(config, mt5_connector, state_engine)))
            
        session = session_engine.get_active_session()
        killzone = session_engine.get_active_killzone()
        
        scan_logs = []
        scan_logs.append(f"⏱️ Manual scan triggered at {datetime.now(timezone.utc).isoformat()}")
        scan_logs.append(f"🌎 Session: {session if session else 'None'} | Killzone: {killzone if killzone else 'None'}")
        
        if not mt5_connector.is_connected():
            if not mt5_connector.connect():
                return jsonify({"success": False, "logs": ["❌ Failed to connect to MetaTrader5."]}), 200
                
        signals_found = 0
        for pair in config.pairs:
            scan_logs.append(f"🔍 Scanning pair: {pair}...")
            # Check pair level restrictions
            if not session_engine.is_pair_allowed(pair, session):
                scan_logs.append(f"   ⚠️ Pair {pair} not allowed in current session.")
                continue
            if state_engine.is_pair_on_cooldown(pair):
                scan_logs.append(f"   ⏳ Pair {pair} is on cooldown.")
                continue
                
            for name, scanner in scanners:
                signal = scanner.scan(pair, session or "London", killzone or "London")
                if signal:
                    signals_found += 1
                    scan_logs.append(f"   ✅ {name} Signal Found for {pair}! {signal['direction']} @ {signal['entry_price']:.5f}")
                    
        scan_logs.append(f"🏁 Scan complete. Found {signals_found} signals.")
        return jsonify({"success": True, "logs": scan_logs})
    except Exception as e:
        return jsonify({"success": False, "logs": [f"❌ Manual Scan Error: {e}"]}), 500

@app.route("/api/backtest/run", methods=["POST"])
def start_backtest():
    global backtest_status, backtest_process
    if backtest_status == "running":
        return jsonify({"error": "Backtest is already running"}), 400
        
    thread = threading.Thread(target=run_backtest_thread)
    thread.daemon = True
    thread.start()
    return jsonify({"success": True, "message": "Backtest started successfully!"})

@app.route("/api/backtest/status", methods=["GET"])
def get_backtest_status():
    global backtest_status, backtest_output
    return jsonify({
        "status": backtest_status,
        "output": "".join(backtest_output)
    })

@app.route("/api/reports", methods=["GET"])
def list_reports():
    if not os.path.exists(BACKTEST_RESULTS_DIR):
        return jsonify([])
        
    try:
        files = os.listdir(BACKTEST_RESULTS_DIR)
        reports = []
        for file in files:
            if file.endswith(".csv"):
                file_path = os.path.join(BACKTEST_RESULTS_DIR, file)
                stat = os.stat(file_path)
                reports.append({
                    "filename": file,
                    "size": f"{stat.st_size / 1024:.1f} KB",
                    "modified": datetime.fromtimestamp(stat.st_mtime).isoformat()
                })
        # Sort reports by modified date desc
        reports.sort(key=lambda x: x["modified"], reverse=True)
        return jsonify(reports[:10])
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/reports/<filename>", methods=["GET"])
def view_report(filename):
    file_path = os.path.join(BACKTEST_RESULTS_DIR, filename)
    if not os.path.exists(file_path) or "../" in filename:
        return "Report file not found", 404
        
    try:
        with open(file_path, "r") as f:
            content = f.read()
        return content, 200, {'Content-Type': 'text/plain'}
    except Exception as e:
        return str(e), 500

# ──────────────────────────────────────────────────────────────────────
# HTML TEMPLATE
# ──────────────────────────────────────────────────────────────────────

HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>VvE FxBOT Dashboard</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&family=Outfit:wght@400;500;600;700;800&display=swap" rel="stylesheet">
    <style>
        :root {
            --bg-primary: #0F121E;
            --bg-card: rgba(22, 28, 48, 0.7);
            --border-card: rgba(255, 255, 255, 0.08);
            --text-primary: #F3F4F6;
            --text-secondary: #9CA3AF;
            --accent-cyan: #00F2FE;
            --accent-blue: #4FACFE;
            --accent-green: #00FF87;
            --accent-red: #FF0844;
            --sidebar-width: 250px;
        }

        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
            font-family: 'Inter', sans-serif;
            scrollbar-width: thin;
            scrollbar-color: rgba(255, 255, 255, 0.1) transparent;
        }

        body {
            background-color: var(--bg-primary);
            color: var(--text-primary);
            min-height: 100vh;
            display: flex;
            overflow-x: hidden;
        }

        /* Sidebar Styling */
        .sidebar {
            width: var(--sidebar-width);
            background: linear-gradient(180deg, #121829 0%, #080B13 100%);
            border-right: 1px solid var(--border-card);
            display: flex;
            flex-direction: column;
            padding: 2rem 1.5rem;
            position: fixed;
            height: 100vh;
            z-index: 10;
        }

        .logo {
            font-family: 'Outfit', sans-serif;
            font-size: 1.6rem;
            font-weight: 800;
            background: linear-gradient(135deg, var(--accent-cyan) 0%, var(--accent-blue) 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            margin-bottom: 3rem;
            display: flex;
            align-items: center;
            gap: 0.5rem;
        }

        .nav-list {
            list-style: none;
            display: flex;
            flex-direction: column;
            gap: 0.75rem;
        }

        .nav-item {
            padding: 0.85rem 1.2rem;
            border-radius: 12px;
            color: var(--text-secondary);
            font-weight: 500;
            cursor: pointer;
            transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
            display: flex;
            align-items: center;
            gap: 1rem;
        }

        .nav-item:hover {
            color: var(--text-primary);
            background: rgba(255, 255, 255, 0.04);
            transform: translateX(3px);
        }

        .nav-item.active {
            color: #FFFFFF;
            background: linear-gradient(135deg, rgba(0, 242, 254, 0.15) 0%, rgba(79, 172, 254, 0.15) 100%);
            border-left: 3px solid var(--accent-cyan);
            padding-left: 1rem;
        }

        /* Main Content Container */
        .main-container {
            margin-left: var(--sidebar-width);
            flex: 1;
            padding: 2.5rem 3.5rem;
            min-height: 100vh;
            display: flex;
            flex-direction: column;
        }

        header {
            margin-bottom: 2.5rem;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }

        h1 {
            font-family: 'Outfit', sans-serif;
            font-size: 2.2rem;
            font-weight: 700;
            letter-spacing: -0.5px;
        }

        /* Panels switcher */
        .panel {
            display: none;
            animation: fadeIn 0.4s ease-out forwards;
        }

        .panel.active {
            display: block;
        }

        @keyframes fadeIn {
            from { opacity: 0; transform: translateY(10px); }
            to { opacity: 1; transform: translateY(0); }
        }

        /* Card Elements */
        .grid-stats {
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 1.5rem;
            margin-bottom: 2.5rem;
        }

        .stat-card {
            background: var(--bg-card);
            border: 1px solid var(--border-card);
            backdrop-filter: blur(12px);
            padding: 1.5rem;
            border-radius: 16px;
            transition: all 0.3s ease;
        }

        .stat-card:hover {
            border-color: rgba(0, 242, 254, 0.3);
            transform: translateY(-2px);
        }

        .stat-label {
            font-size: 0.85rem;
            color: var(--text-secondary);
            margin-bottom: 0.5rem;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }

        .stat-value {
            font-size: 1.8rem;
            font-weight: 700;
            color: #FFFFFF;
            font-family: 'Outfit', sans-serif;
        }

        .card {
            background: var(--bg-card);
            border: 1px solid var(--border-card);
            backdrop-filter: blur(12px);
            padding: 2rem;
            border-radius: 16px;
            margin-bottom: 2rem;
        }

        .card-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 1.5rem;
            border-bottom: 1px solid rgba(255, 255, 255, 0.05);
            padding-bottom: 1rem;
        }

        .card-title {
            font-family: 'Outfit', sans-serif;
            font-size: 1.3rem;
            font-weight: 600;
        }

        /* Tables */
        table {
            width: 100%;
            border-collapse: collapse;
            text-align: left;
        }

        th {
            color: var(--text-secondary);
            font-size: 0.85rem;
            font-weight: 600;
            padding: 1rem;
            border-bottom: 1px solid var(--border-card);
        }

        td {
            padding: 1rem;
            border-bottom: 1px solid rgba(255, 255, 255, 0.03);
            font-size: 0.9rem;
        }

        tr:last-child td {
            border-bottom: none;
        }

        .badge {
            display: inline-block;
            padding: 0.25rem 0.6rem;
            border-radius: 20px;
            font-size: 0.75rem;
            font-weight: 600;
        }

        .badge-win { background: rgba(0, 255, 135, 0.1); color: var(--accent-green); }
        .badge-loss { background: rgba(255, 8, 68, 0.1); color: var(--accent-red); }
        .badge-open { background: rgba(79, 172, 254, 0.1); color: var(--accent-blue); }
        .badge-be { background: rgba(156, 163, 175, 0.1); color: var(--text-secondary); }

        /* Configuration Forms */
        .form-grid {
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            gap: 1.5rem;
        }

        .form-group {
            display: flex;
            flex-direction: column;
            gap: 0.5rem;
        }

        label {
            font-size: 0.85rem;
            font-weight: 600;
            color: var(--text-secondary);
        }

        input, select, textarea {
            background: rgba(255, 255, 255, 0.03);
            border: 1px solid var(--border-card);
            border-radius: 8px;
            padding: 0.75rem 1rem;
            color: #FFFFFF;
            font-size: 0.9rem;
            transition: all 0.3s ease;
        }

        input:focus, select:focus, textarea:focus {
            outline: none;
            border-color: var(--accent-cyan);
            box-shadow: 0 0 10px rgba(0, 242, 254, 0.1);
        }

        /* Buttons */
        .btn {
            background: linear-gradient(135deg, var(--accent-cyan) 0%, var(--accent-blue) 100%);
            border: none;
            color: #000000;
            font-weight: 700;
            font-size: 0.9rem;
            padding: 0.75rem 1.5rem;
            border-radius: 8px;
            cursor: pointer;
            transition: all 0.3s ease;
            display: inline-flex;
            align-items: center;
            gap: 0.5rem;
        }

        .btn:hover {
            transform: translateY(-1px);
            box-shadow: 0 5px 15px rgba(0, 242, 254, 0.3);
        }

        .btn:active {
            transform: translateY(0);
        }

        .btn-secondary {
            background: rgba(255, 255, 255, 0.05);
            border: 1px solid var(--border-card);
            color: var(--text-primary);
        }

        .btn-secondary:hover {
            background: rgba(255, 255, 255, 0.08);
            box-shadow: none;
        }

        .btn-red {
            background: linear-gradient(135deg, #FF0844 0%, #FFB199 100%);
            color: #FFFFFF;
        }

        .btn-red:hover {
            box-shadow: 0 5px 15px rgba(255, 8, 68, 0.3);
        }

        /* Console Output monitor */
        .console-log {
            background: #070913;
            border: 1px solid var(--border-card);
            border-radius: 12px;
            padding: 1.5rem;
            font-family: 'Courier New', Courier, monospace;
            font-size: 0.85rem;
            color: #00FF87;
            height: 350px;
            overflow-y: auto;
            white-space: pre-wrap;
            margin-bottom: 1.5rem;
        }

        .flex-actions {
            display: flex;
            gap: 1rem;
            margin-bottom: 1.5rem;
        }
        
        .tab-buttons {
            display: flex;
            gap: 0.5rem;
            background: rgba(255,255,255,0.03);
            padding: 0.25rem;
            border-radius: 8px;
            border: 1px solid var(--border-card);
            margin-bottom: 1rem;
            max-width: fit-content;
        }
        
        .tab-btn {
            padding: 0.5rem 1rem;
            border-radius: 6px;
            cursor: pointer;
            font-size: 0.8rem;
            font-weight: 600;
            color: var(--text-secondary);
            transition: all 0.2s ease;
        }
        
        .tab-btn.active {
            background: rgba(255,255,255,0.06);
            color: #FFFFFF;
        }
    </style>
</head>
<body>

    <!-- Sidebar -->
    <div class="sidebar">
        <div class="logo">🧬 VvE FxBOT</div>
        <ul class="nav-list">
            <li class="nav-item active" onclick="switchPanel('home-panel', this)">📊 Home Dashboard</li>
            <li class="nav-item" onclick="switchPanel('config-panel', this)">⚙️ Settings / Config</li>
            <li class="nav-item" onclick="switchPanel('trades-panel', this)">📜 Logged Trades</li>
            <li class="nav-item" onclick="switchPanel('backtest-panel', this)">🧪 Backtest & Scan</li>
        </ul>
    </div>

    <!-- Main Container -->
    <div class="main-container">
        <header>
            <div>
                <h1 id="header-title">Home Dashboard</h1>
                <p style="color: var(--text-secondary); font-size: 0.9rem; margin-top: 0.25rem;">Live terminal connection monitoring and trade management.</p>
            </div>
            <div>
                <button class="btn" onclick="triggerScan()"><span style="font-size: 1.1rem;">⚡</span> Run Live Scan</button>
            </div>
        </header>

        <!-- ────────────────────────────────────────── PANEL: HOME ────────────────────────────────────────── -->
        <div id="home-panel" class="panel active">
            <div class="grid-stats">
                <div class="stat-card">
                    <div class="stat-label">Total Trades Logged</div>
                    <div class="stat-value" id="stat-total-trades">0</div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">Win Rate (Closed)</div>
                    <div class="stat-value" id="stat-winrate">0.0%</div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">Active Open Trades</div>
                    <div class="stat-value" id="stat-active-trades" style="color: var(--accent-blue);">0</div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">Total Skipped Signals</div>
                    <div class="stat-value" id="stat-skipped-trades" style="color: var(--text-secondary);">0</div>
                </div>
            </div>

            <!-- Live Open Trades -->
            <div class="card">
                <div class="card-header">
                    <div class="card-title">Live Active Open Trades</div>
                    <button class="btn btn-secondary" onclick="loadLiveTrades()">Refresh</button>
                </div>
                <table id="table-live-trades">
                    <thead>
                        <tr>
                            <th>Ticket</th>
                            <th>Pair</th>
                            <th>Direction</th>
                            <th>Lots</th>
                            <th>Entry Price</th>
                            <th>SL</th>
                            <th>TP1</th>
                            <th>TP2</th>
                            <th>Status</th>
                        </tr>
                    </thead>
                    <tbody>
                        <!-- Injected by JS -->
                    </tbody>
                </table>
            </div>
        </div>

        <!-- ────────────────────────────────────────── PANEL: CONFIG ────────────────────────────────────────── -->
        <div id="config-panel" class="panel">
            <div class="card">
                <div class="card-header">
                    <div class="card-title">Trading Bot configuration (config.json)</div>
                    <button class="btn" onclick="saveConfig()">Save Settings</button>
                </div>
                <form id="config-form" class="form-grid">
                    <div class="form-group">
                        <label>Strategy Mode</label>
                        <select id="cfg-strategy_mode">
                            <option value="MMXM">MMXM Only</option>
                            <option value="OTE">OTE Only</option>
                            <option value="MULTI">MULTI Concurrency</option>
                        </select>
                    </div>
                    <div class="form-group">
                        <label>Risk Percent (%)</label>
                        <input type="number" step="0.1" id="cfg-risk_percent">
                    </div>
                    <div class="form-group">
                        <label>Max Trades Per Day</label>
                        <input type="number" id="cfg-max_trades_day">
                    </div>
                    <div class="form-group">
                        <label>Max Trades Per Pair Per Day</label>
                        <input type="number" id="cfg-max_trades_pair_day">
                    </div>
                    <div class="form-group">
                        <label>Max Open Trades</label>
                        <input type="number" id="cfg-max_open_trades">
                    </div>
                    <div class="form-group">
                        <label>Trading Pool Size ($)</label>
                        <input type="number" id="cfg-trading_pool_size">
                    </div>
                    <div class="form-group">
                        <label>Enabled Scanners</label>
                        <div style="display: flex; gap: 1rem; align-items: center; padding: 0.5rem 0;">
                            <label><input type="checkbox" id="cfg-scanner-mmxm"> MMXM</label>
                            <label><input type="checkbox" id="cfg-scanner-ote"> OTE</label>
                        </div>
                    </div>
                    <div class="form-group">
                        <label>Pairs (comma-separated list)</label>
                        <input type="text" id="cfg-pairs">
                    </div>
                    
                    <div style="grid-column: span 2; margin-top: 1.5rem;">
                        <h3 class="card-title" style="margin-bottom: 1rem; border-bottom: 1px solid rgba(255,255,255,0.05); padding-bottom: 0.5rem;">OTE Scanner Settings</h3>
                    </div>
                    
                    <div class="form-group">
                        <label>OTE Signals Timeframe</label>
                        <input type="text" id="cfg-ote-timeframe_signal">
                    </div>
                    <div class="form-group">
                        <label>OTE Trigger Timeframe</label>
                        <input type="text" id="cfg-ote-timeframe_trigger">
                    </div>
                    <div class="form-group">
                        <label>OTE Fib Target Min</label>
                        <input type="number" step="0.001" id="cfg-ote-fib_min">
                    </div>
                    <div class="form-group">
                        <label>OTE Fib Target Max</label>
                        <input type="number" step="0.001" id="cfg-ote-fib_max">
                    </div>
                    <div class="form-group">
                        <label>OTE SL (Points)</label>
                        <input type="number" id="cfg-ote-sl_points">
                    </div>
                    <div class="form-group">
                        <label>OTE TP (Points)</label>
                        <input type="number" id="cfg-ote-tp_points">
                    </div>
                    <div class="form-group">
                        <label>OTE Max Daily Trades (per-pair)</label>
                        <input type="number" id="cfg-ote-max_daily_trades">
                    </div>
                    <div class="form-group">
                        <label>OTE Cooldown (Minutes)</label>
                        <input type="number" id="cfg-ote-cooldown_minutes">
                    </div>
                </form>
            </div>
        </div>

        <!-- ────────────────────────────────────────── PANEL: TRADES LOGS ────────────────────────────────────────── -->
        <div id="trades-panel" class="panel">
            <div class="tab-buttons">
                <div class="tab-btn active" onclick="switchTradeTab('executed', this)">Executed Trades</div>
                <div class="tab-btn" onclick="switchTradeTab('skipped', this)">Skipped / Denied Signals</div>
            </div>

            <!-- Executed Trades Container -->
            <div id="executed-trades-container" class="card">
                <div class="card-header">
                    <div class="card-title">Historical Executed Deals</div>
                    <button class="btn btn-secondary" onclick="loadTrades()">Refresh logs</button>
                </div>
                <div style="overflow-x: auto;">
                    <table id="table-executed">
                        <thead>
                            <tr>
                                <th>Ticket</th>
                                <th>Pair</th>
                                <th>Direction</th>
                                <th>Lots</th>
                                <th>Execution Time</th>
                                <th>SL</th>
                                <th>TP1</th>
                                <th>TP2</th>
                                <th>Profit</th>
                                <th>Result</th>
                            </tr>
                        </thead>
                        <tbody>
                            <!-- Injected by JS -->
                        </tbody>
                    </table>
                </div>
            </div>

            <!-- Skipped Trades Container -->
            <div id="skipped-trades-container" class="card" style="display: none;">
                <div class="card-header">
                    <div class="card-title">Skipped / Denied Signals</div>
                    <button class="btn btn-secondary" onclick="loadTrades()">Refresh logs</button>
                </div>
                <div style="overflow-x: auto;">
                    <table id="table-skipped">
                        <thead>
                            <tr>
                                <th>Signal ID</th>
                                <th>Reason for Skip / Denial</th>
                                <th>Time Rejected</th>
                                <th>Spread</th>
                                <th>Signal Score</th>
                            </tr>
                        </thead>
                        <tbody>
                            <!-- Injected by JS -->
                        </tbody>
                    </table>
                </div>
            </div>
        </div>

        <!-- ────────────────────────────────────────── PANEL: BACKTEST ────────────────────────────────────────── -->
        <div id="backtest-panel" class="panel">
            <div class="card">
                <div class="card-header">
                    <div class="card-title">Chronological Replay Backtest Runner</div>
                </div>
                <div class="flex-actions">
                    <button class="btn" id="btn-run-backtest" onclick="runBacktest()">▶ Run Full Backtest</button>
                    <button class="btn btn-secondary" onclick="checkBacktestStatus()">Refresh Console</button>
                </div>
                <div class="console-log" id="backtest-console">Console is idle. Press 'Run Full Backtest' to start backtesting...</div>
            </div>

            <!-- Reports and files -->
            <div class="card">
                <div class="card-header">
                    <div class="card-title">Generated Backtesting Performance Reports</div>
                    <button class="btn btn-secondary" onclick="loadReports()">List Reports</button>
                </div>
                <table id="table-reports">
                    <thead>
                        <tr>
                            <th>Report Name</th>
                            <th>File Size</th>
                            <th>Created / Modified</th>
                            <th>Action</th>
                        </tr>
                    </thead>
                    <tbody>
                        <!-- Injected by JS -->
                    </tbody>
                </table>
            </div>
            
            <div class="card" id="report-view-card" style="display: none;">
                <div class="card-header">
                    <div class="card-title" id="report-view-title">Viewing Report Content</div>
                    <button class="btn btn-secondary" onclick="document.getElementById('report-view-card').style.display='none'">Close</button>
                </div>
                <pre class="console-log" id="report-view-content" style="height: 500px; color: #FFFFFF; font-family: monospace;"></pre>
            </div>
        </div>

    </div>

    <!-- JavaScript logic -->
    <script>
        let fullConfig = {};
        let backtestTimer = null;

        document.addEventListener("DOMContentLoaded", () => {
            loadConfig();
            loadTrades();
            loadLiveTrades();
            loadReports();
            
            // Poll backtest status every 3 seconds
            setInterval(checkBacktestStatus, 3000);
        });

        function switchPanel(panelId, element) {
            document.querySelectorAll(".panel").forEach(p => p.classList.remove("active"));
            document.getElementById(panelId).classList.add("active");
            
            document.querySelectorAll(".nav-item").forEach(item => item.classList.remove("active"));
            element.classList.add("active");

            // Update title
            const headers = {
                'home-panel': 'Home Dashboard',
                'config-panel': 'Configuration Engine',
                'trades-panel': 'Historical Signal Logs',
                'backtest-panel': 'Replay Backtester & Manual Scan'
            };
            document.getElementById("header-title").innerText = headers[panelId];
        }

        function switchTradeTab(tabName, element) {
            document.querySelectorAll(".tab-btn").forEach(btn => btn.classList.remove("active"));
            element.classList.add("active");
            
            if (tabName === 'executed') {
                document.getElementById("executed-trades-container").style.display = "block";
                document.getElementById("skipped-trades-container").style.display = "none";
            } else {
                document.getElementById("executed-trades-container").style.display = "none";
                document.getElementById("skipped-trades-container").style.display = "block";
            }
        }

        // API: Config
        async function loadConfig() {
            try {
                const response = await fetch("/api/config");
                fullConfig = await response.json();
                
                document.getElementById("cfg-strategy_mode").value = fullConfig.strategy_mode;
                document.getElementById("cfg-risk_percent").value = fullConfig.risk_percent;
                document.getElementById("cfg-max_trades_day").value = fullConfig.max_trades_day;
                document.getElementById("cfg-max_trades_pair_day").value = fullConfig.max_trades_pair_day;
                document.getElementById("cfg-max_open_trades").value = fullConfig.max_open_trades;
                document.getElementById("cfg-trading_pool_size").value = fullConfig.trading_pool_size;
                document.getElementById("cfg-pairs").value = fullConfig.pairs.join(", ");
                
                document.getElementById("cfg-scanner-mmxm").checked = fullConfig.enabled_scanners.mmxm;
                document.getElementById("cfg-scanner-ote").checked = fullConfig.enabled_scanners.ote;
                
                if (fullConfig.ote_scanner) {
                    document.getElementById("cfg-ote-timeframe_signal").value = fullConfig.ote_scanner.timeframe_signal || "H1";
                    document.getElementById("cfg-ote-timeframe_trigger").value = fullConfig.ote_scanner.timeframe_trigger || "M5";
                    document.getElementById("cfg-ote-fib_min").value = fullConfig.ote_scanner.fib_min || 0.618;
                    document.getElementById("cfg-ote-fib_max").value = fullConfig.ote_scanner.fib_max || 0.705;
                    document.getElementById("cfg-ote-sl_points").value = fullConfig.ote_scanner.sl_points || 150;
                    document.getElementById("cfg-ote-tp_points").value = fullConfig.ote_scanner.tp_points || 450;
                    document.getElementById("cfg-ote-max_daily_trades").value = fullConfig.ote_scanner.max_daily_trades || 5;
                    document.getElementById("cfg-ote-cooldown_minutes").value = fullConfig.ote_scanner.cooldown_minutes || 15;
                }
            } catch (err) {
                alert("Failed to load config.json: " + err);
            }
        }

        async function saveConfig() {
            try {
                // Map values
                fullConfig.strategy_mode = document.getElementById("cfg-strategy_mode").value;
                fullConfig.risk_percent = parseFloat(document.getElementById("cfg-risk_percent").value);
                fullConfig.max_trades_day = parseInt(document.getElementById("cfg-max_trades_day").value);
                fullConfig.max_trades_pair_day = parseInt(document.getElementById("cfg-max_trades_pair_day").value);
                fullConfig.max_open_trades = parseInt(document.getElementById("cfg-max_open_trades").value);
                fullConfig.trading_pool_size = parseFloat(document.getElementById("cfg-trading_pool_size").value);
                
                fullConfig.enabled_scanners.mmxm = document.getElementById("cfg-scanner-mmxm").checked;
                fullConfig.enabled_scanners.ote = document.getElementById("cfg-scanner-ote").checked;
                
                fullConfig.pairs = document.getElementById("cfg-pairs").value.split(",").map(p => p.trim()).filter(p => p);
                
                if (!fullConfig.ote_scanner) fullConfig.ote_scanner = {};
                
                fullConfig.ote_scanner.timeframe_signal = document.getElementById("cfg-ote-timeframe_signal").value;
                fullConfig.ote_scanner.timeframe_trigger = document.getElementById("cfg-ote-timeframe_trigger").value;
                fullConfig.ote_scanner.fib_min = parseFloat(document.getElementById("cfg-ote-fib_min").value);
                fullConfig.ote_scanner.fib_max = parseFloat(document.getElementById("cfg-ote-fib_max").value);
                fullConfig.ote_scanner.sl_points = parseInt(document.getElementById("cfg-ote-sl_points").value);
                fullConfig.ote_scanner.tp_points = parseInt(document.getElementById("cfg-ote-tp_points").value);
                fullConfig.ote_scanner.max_daily_trades = parseInt(document.getElementById("cfg-ote-max_daily_trades").value);
                fullConfig.ote_scanner.cooldown_minutes = parseInt(document.getElementById("cfg-ote-cooldown_minutes").value);
                
                const response = await fetch("/api/config", {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify(fullConfig)
                });
                
                const result = await response.json();
                if (result.success) {
                    alert("Configuration saved successfully!");
                } else {
                    alert("Save failed: " + result.error);
                }
            } catch (err) {
                alert("Error saving configuration: " + err);
            }
        }

        // API: Trades
        async function loadTrades() {
            try {
                const response = await fetch("/api/trades");
                const data = await response.json();
                
                // Stats updates
                const totalTrades = (data.executed || []).length;
                document.getElementById("stat-total-trades").innerText = totalTrades;
                
                const winCount = (data.executed || []).filter(t => t.result === 'WIN').length;
                const closedCount = (data.executed || []).filter(t => t.status === 'CLOSED').length;
                const winrate = closedCount > 0 ? ((winCount / closedCount) * 100).toFixed(1) : "0.0";
                document.getElementById("stat-winrate").innerText = winrate + "%";
                
                document.getElementById("stat-skipped-trades").innerText = (data.skipped || []).length;

                // Render Executed
                const execBody = document.querySelector("#table-executed tbody");
                execBody.innerHTML = "";
                if (data.executed && data.executed.length > 0) {
                    data.executed.forEach(t => {
                        let badgeClass = 'badge-be';
                        if (t.result === 'WIN') badgeClass = 'badge-win';
                        if (t.result === 'LOSS') badgeClass = 'badge-loss';
                        if (t.status === 'OPEN') badgeClass = 'badge-open';

                        execBody.innerHTML += `
                            <tr>
                                <td><code>${t.ticket || t.trade_id.slice(0, 8)}</code></td>
                                <td><strong>${t.pair}</strong></td>
                                <td><span style="color: ${t.direction === 'BUY' ? 'var(--accent-cyan)' : 'var(--accent-red)'}; font-weight: 600;">${t.direction}</span></td>
                                <td>${t.lot_total}</td>
                                <td>${t.execution_time ? t.execution_time.slice(0, 19).replace('T', ' ') : ''}</td>
                                <td>${t.sl ? t.sl.toFixed(5) : ''}</td>
                                <td>${t.tp1 ? t.tp1.toFixed(5) : ''}</td>
                                <td>${t.tp2 ? t.tp2.toFixed(5) : ''}</td>
                                <td style="font-weight: bold; color: ${t.profit_usd >= 0 ? 'var(--accent-green)' : 'var(--accent-red)'}">
                                    ${t.profit_usd >= 0 ? '+' : ''}${t.profit_usd ? t.profit_usd.toFixed(2) : '0.00'}
                                </td>
                                <td><span class="badge ${badgeClass}">${t.status === 'OPEN' ? 'OPEN' : t.result}</span></td>
                            </tr>
                        `;
                    });
                } else {
                    execBody.innerHTML = "<tr><td colspan='10' style='text-align: center; color: var(--text-secondary);'>No trades found in SQLite DB database.</td></tr>";
                }

                // Render Skipped
                const skippedBody = document.querySelector("#table-skipped tbody");
                skippedBody.innerHTML = "";
                if (data.skipped && data.skipped.length > 0) {
                    data.skipped.forEach(t => {
                        skippedBody.innerHTML += `
                            <tr>
                                <td><code>${t.signal_id.slice(0, 8)}</code></td>
                                <td><span style="color: #FF5A5F; font-weight: 500;">${t.reason}</span></td>
                                <td>${t.skip_time ? t.skip_time.slice(0, 19).replace('T', ' ') : ''}</td>
                                <td>${t.spread ? t.spread.toFixed(1) : '0.0'}</td>
                                <td>${t.score ? t.score.toFixed(1) : '0.0'}</td>
                            </tr>
                        `;
                    });
                } else {
                    skippedBody.innerHTML = "<tr><td colspan='5' style='text-align: center; color: var(--text-secondary);'>No skipped/denied setups found in database.</td></tr>";
                }

            } catch (err) {
                console.error("Trades loader failed", err);
            }
        }

        async function loadLiveTrades() {
            try {
                const response = await fetch("/api/live_trades");
                const data = await response.json();
                
                document.getElementById("stat-active-trades").innerText = data.length;

                const body = document.querySelector("#table-live-trades tbody");
                body.innerHTML = "";
                if (data && data.length > 0) {
                    data.forEach(t => {
                        body.innerHTML += `
                            <tr>
                                <td><code>${t.ticket || t.trade_id.slice(0, 8)}</code></td>
                                <td><strong>${t.pair}</strong></td>
                                <td><span style="color: ${t.direction === 'BUY' ? 'var(--accent-cyan)' : 'var(--accent-red)'}; font-weight: 600;">${t.direction}</span></td>
                                <td>${t.lot_total}</td>
                                <td>${t.executed_price ? t.executed_price.toFixed(5) : ''}</td>
                                <td>${t.sl ? t.sl.toFixed(5) : ''}</td>
                                <td>${t.tp1 ? t.tp1.toFixed(5) : ''}</td>
                                <td>${t.tp2 ? t.tp2.toFixed(5) : ''}</td>
                                <td><span class="badge badge-open">ACTIVE</span></td>
                            </tr>
                        `;
                    });
                } else {
                    body.innerHTML = "<tr><td colspan='9' style='text-align: center; color: var(--text-secondary);'>No live trades are currently active in SQLite.</td></tr>";
                }
            } catch (err) {
                console.error("Failed to load active trades", err);
            }
        }

        // Live Scan Trigger
        async function triggerScan() {
            const btn = document.querySelector("header button");
            btn.innerText = "⚡ Scanning...";
            btn.disabled = true;
            
            try {
                const response = await fetch("/api/scan", { method: "POST" });
                const data = await response.json();
                
                alert("Live scan complete!\n" + data.logs.join("\n"));
                loadLiveTrades();
                loadTrades();
            } catch (err) {
                alert("Scan failed: " + err);
            } finally {
                btn.innerText = "⚡ Run Live Scan";
                btn.disabled = false;
            }
        }

        // API: Backtest subprocess
        async function runBacktest() {
            const btn = document.getElementById("btn-run-backtest");
            btn.innerText = "⏳ Running Replay...";
            btn.disabled = true;
            
            try {
                const response = await fetch("/api/backtest/run", { method: "POST" });
                const result = await response.json();
                if (result.success) {
                    document.getElementById("backtest-console").innerText = "🚀 Backtesting thread started...";
                    checkBacktestStatus();
                } else {
                    alert(result.error);
                    btn.innerText = "▶ Run Full Backtest";
                    btn.disabled = false;
                }
            } catch (err) {
                alert("Backtest failed: " + err);
                btn.innerText = "▶ Run Full Backtest";
                btn.disabled = false;
            }
        }

        async function checkBacktestStatus() {
            try {
                const response = await fetch("/api/backtest/status");
                const data = await response.json();
                
                const consoleDiv = document.getElementById("backtest-console");
                consoleDiv.innerText = data.output || "Console is idle.";
                consoleDiv.scrollTop = consoleDiv.scrollHeight; // Auto-scroll to bottom

                const btn = document.getElementById("btn-run-backtest");
                if (data.status === "running") {
                    btn.innerText = "⏳ Running Replay...";
                    btn.disabled = true;
                } else {
                    btn.innerText = "▶ Run Full Backtest";
                    btn.disabled = false;
                    
                    if (data.status === "completed" || data.status === "failed") {
                        loadReports(); // reload reports automatically
                    }
                }
            } catch (err) {
                console.error("Failed to check status", err);
            }
        }

        // Reports
        async function loadReports() {
            try {
                const response = await fetch("/api/reports");
                const data = await response.json();
                
                const body = document.querySelector("#table-reports tbody");
                body.innerHTML = "";
                if (data && data.length > 0) {
                    data.forEach(r => {
                        body.innerHTML += `
                            <tr>
                                <td><strong>${r.filename}</strong></td>
                                <td>${r.size}</td>
                                <td>${r.modified.slice(0, 19).replace('T', ' ')}</td>
                                <td>
                                    <button class="btn btn-secondary" style="padding: 0.35rem 0.75rem; font-size: 0.75rem;" onclick="viewReport('${r.filename}')">👁️ View</button>
                                </td>
                            </tr>
                        `;
                    });
                } else {
                    body.innerHTML = "<tr><td colspan='4' style='text-align: center; color: var(--text-secondary);'>No generated reports found. Run a backtest first.</td></tr>";
                }
            } catch (err) {
                console.error("Failed to load reports", err);
            }
        }

        async function viewReport(filename) {
            try {
                const response = await fetch(`/api/reports/${filename}`);
                const content = await response.text();
                
                document.getElementById("report-view-title").innerText = `Viewing Report: ${filename}`;
                document.getElementById("report-view-content").innerText = content;
                document.getElementById("report-view-card").style.display = "block";
                
                // Scroll to top of report view
                document.getElementById("report-view-card").scrollIntoView({ behavior: 'smooth' });
            } catch (err) {
                alert("Failed to load report content: " + err);
            }
        }
    </script>
</body>
</html>
"""
