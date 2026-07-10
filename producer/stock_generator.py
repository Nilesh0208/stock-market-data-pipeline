import random

STOCKS = [
    {
        "symbol": "AAPL",
        "company": "Apple Inc",
        "exchange": "NASDAQ",
        "currency": "USD",
        "base_price": 195.00
    },
    {
        "symbol": "MSFT",
        "company": "Microsoft Corporation",
        "exchange": "NASDAQ",
        "currency": "USD",
        "base_price": 510.00
    },
    {
        "symbol": "GOOGL",
        "company": "Alphabet Inc",
        "exchange": "NASDAQ",
        "currency": "USD",
        "base_price": 179.00
    },
    {
        "symbol": "AMZN",
        "company": "Amazon.com Inc",
        "exchange": "NASDAQ",
        "currency": "USD",
        "base_price": 225.00
    },
    {
        "symbol": "TSLA",
        "company": "Tesla Inc",
        "exchange": "NASDAQ",
        "currency": "USD",
        "base_price": 315.00
    }
]


def generate_stock():
    """
    Generate a random stock market event.
    """

    # Select a random stock
    stock = random.choice(STOCKS)

    # Generate a realistic price around the stock's base price
    price = round(
        stock["base_price"] + random.uniform(-5, 5),
        2
    )

    # Generate a random trading volume
    volume = random.randint(1000, 10000)

    # Return stock data
    return {
        "symbol": stock["symbol"],
        "company": stock["company"],
        "exchange": stock["exchange"],
        "currency": stock["currency"],
        "price": price,
        "volume": volume
    }