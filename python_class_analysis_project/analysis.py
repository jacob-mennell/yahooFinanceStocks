from StockExplore import ExploreStocks

#test function
stocks = ExploreStocks(['IAG.L', '0293.HK', 'AF.PA'], '25y')

#lets look at df
stocks.stock_history

#plot stock price over time
stocks.plot_stock_price()

#plot trade volumne over time
stocks.plot_trade_volume()

#plot stock volatility
stocks.plot_volatility()

#plot rolling average
stocks.plot_rolling_average()

#plot cumulative return over time
stocks.plot_cumulative_returns()

#test plot trend function
stocks.plot_future_trend('AF.PA')