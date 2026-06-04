import pandas as pd
import numpy as np
import argparse
import os

def load_data(csv_path):
    df = pd.read_csv(csv_path)
    df['open_time'] = pd.to_datetime(df['open_time'])
    df['close_time'] = pd.to_datetime(df['close_time'])
    df['year'] = df['open_time'].dt.year
    df['month'] = df['open_time'].dt.to_period('M')
    df['week'] = df['open_time'].dt.to_period('W')
    
    # Map strategies
    strategy_map = {
        'Leg A': 'Strategy A (0 GMT Liquidity)',
        'ZGMT-EXCEPTION': 'Strategy B (0 GMT + OB)',
        'Leg B': 'Strategy C (Manipulation/Judas)'
    }
    df['strategy'] = df['entry_leg'].map(strategy_map).fillna('Other')
    return df

def generate_pivot_1(df):
    stats = []
    for strat in ['Strategy A (0 GMT Liquidity)', 'Strategy B (0 GMT + OB)', 'Strategy C (Manipulation/Judas)']:
        strat_df = df[df['strategy'] == strat]
        total = len(strat_df)
        wins = len(strat_df[strat_df['result'] == 'WIN'])
        losses = len(strat_df[strat_df['result'] == 'LOSS'])
        wr = (wins / total * 100) if total > 0 else 0
        net_profit = strat_df['profit_usd'].sum()
        
        gross_profit = strat_df[strat_df['profit_usd'] > 0]['profit_usd'].sum()
        gross_loss = abs(strat_df[strat_df['profit_usd'] < 0]['profit_usd'].sum())
        pf = (gross_profit / gross_loss) if gross_loss > 0 else (gross_profit if gross_profit > 0 else 0)
        
        strat_df = strat_df.sort_values('open_time')
        equity = strat_df['profit_usd'].cumsum()
        peak = equity.cummax()
        drawdown = peak - equity
        max_dd = drawdown.max() if len(drawdown) > 0 else 0
        
        expectancy = net_profit / total if total > 0 else 0
        
        stats.append({
            'Strategy': strat,
            'Total Trades': total,
            'Winning Trades': wins,
            'Losing Trades': losses,
            'Win Rate %': f"{wr:.1f}%",
            'Net Profit ($)': f"${net_profit:.2f}",
            'Profit Factor': f"{pf:.2f}",
            'Max Drawdown ($)': f"${max_dd:.2f}",
            'Expectancy': f"${expectancy:.2f}"
        })
    return pd.DataFrame(stats).set_index('Strategy').T

def generate_pivot_2(df):
    pivot = df.pivot_table(index='month', columns='strategy', values=['result', 'profit_usd'], 
                           aggfunc={'result': lambda x: (x == 'WIN').mean() * 100, 'profit_usd': 'sum'}).fillna(0)
    pivot.columns = [f"{col[1]} {col[0]}" for col in pivot.columns]
    return pivot

def generate_pivot_3(df):
    pivot = df.pivot_table(index='session', columns='strategy', values=['result', 'trade_id'], 
                           aggfunc={'result': lambda x: (x == 'WIN').mean() * 100, 'trade_id': 'count'}).fillna(0)
    pivot.columns = [f"{col[1]} {col[0]}" for col in pivot.columns]
    return pivot

def generate_pivot_4(df):
    pivot = df.pivot_table(index='pair', columns='strategy', values=['result', 'profit_usd'], 
                           aggfunc={'result': lambda x: (x == 'WIN').mean() * 100, 'profit_usd': 'sum'}).fillna(0)
    pivot.columns = [f"{col[1]} {col[0]}" for col in pivot.columns]
    return pivot

def generate_pivot_5(df):
    pivot = df.pivot_table(index='year', columns='strategy', values=['result', 'profit_usd'], 
                           aggfunc={'result': lambda x: (x == 'WIN').mean() * 100, 'profit_usd': 'sum'}).fillna(0)
    pivot.columns = [f"{col[1]} {col[0]}" for col in pivot.columns]
    return pivot

def generate_pivot_6(df):
    df_sorted = df.sort_values('open_time')
    equity_df = pd.DataFrame({'Date': df_sorted['open_time']})
    for strat in ['Strategy A (0 GMT Liquidity)', 'Strategy B (0 GMT + OB)', 'Strategy C (Manipulation/Judas)']:
        strat_pnl = df_sorted[df_sorted['strategy'] == strat]['profit_usd']
        equity_df[f"{strat} Balance"] = strat_pnl.cumsum().fillna(method='ffill').fillna(0) + 10000
    
    # Resample to monthly ends for the pivot
    equity_df = equity_df.set_index('Date').resample('M').last().fillna(method='ffill')
    return equity_df

def generate_monthly_performance(df):
    monthly = []
    for strat in ['Strategy A (0 GMT Liquidity)', 'Strategy B (0 GMT + OB)', 'Strategy C (Manipulation/Judas)']:
        strat_df = df[df['strategy'] == strat]
        for month, grp in strat_df.groupby('month'):
            total = len(grp)
            wr = (len(grp[grp['result'] == 'WIN']) / total * 100) if total > 0 else 0
            net_profit = grp['profit_usd'].sum()
            gross_p = grp[grp['profit_usd'] > 0]['profit_usd'].sum()
            gross_l = abs(grp[grp['profit_usd'] < 0]['profit_usd'].sum())
            pf = (gross_p / gross_l) if gross_l > 0 else (gross_p if gross_p > 0 else 0)
            monthly.append({
                'Strategy': strat,
                'Month': str(month),
                'Total Trades': total,
                'Win Rate': f"{wr:.1f}%",
                'Net Profit': f"${net_profit:.2f}",
                'Profit Factor': f"{pf:.2f}"
            })
    return pd.DataFrame(monthly)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--csv', required=True, help='Path to backtest CSV')
    parser.add_argument('--out', default='backtest/results/backtest_report.xlsx', help='Output Excel path')
    args = parser.parse_args()

    print(f"Loading data from {args.csv}")
    df = load_data(args.csv)

    print("Generating reports...")
    with pd.ExcelWriter(args.out) as writer:
        generate_pivot_1(df).to_excel(writer, sheet_name='Pivot 1_ Summary')
        generate_pivot_2(df).to_excel(writer, sheet_name='Pivot 2_ Monthly')
        generate_pivot_3(df).to_excel(writer, sheet_name='Pivot 3_ Session')
        generate_pivot_4(df).to_excel(writer, sheet_name='Pivot 4_ Pairs')
        generate_pivot_5(df).to_excel(writer, sheet_name='Pivot 5_ YoY')
        generate_pivot_6(df).to_excel(writer, sheet_name='Pivot 6_ Equity Curve')
        generate_monthly_performance(df).to_excel(writer, sheet_name='Monthly Performance', index=False)

    print(f"✅ Excel report saved to: {args.out}")

if __name__ == "__main__":
    main()
