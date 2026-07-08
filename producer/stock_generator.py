import random
from datetime import datetime

STOCKS = {
    "AAPL": 190,
    "GOOGL": 175,
    "MSFT": 450,
    "AMZN": 185,
    "TSLA": 250,
    "NVDA": 125,
    "META": 520
}


def generate_stock():
    symbol = random.choice(list(STOCKS.keys()))

    base_price = STOCKS[symbol]

    price = round(
        base_price + random.uniform(-5, 5),
        2
    )

    volume = random.randint(
        100,
        10000
    )

    return {
        "symbol": symbol,
        "price": price,
        "volume": volume,
        "timestamp": datetime.utcnow().isoformat()
    }