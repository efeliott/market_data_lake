import os
import requests
from dotenv import load_dotenv

# Load .env at repo root so local tests pick up secrets
load_dotenv()

api_key = os.environ.get("TWELVEDATA_API_KEY")


def check(url: str):
    """Hit an upstream API endpoint and print a short diagnostic snapshot."""
    r = requests.get(url, timeout=30)
    print("=" * 80)
    print("GET", r.url)
    print("status:", r.status_code)
    r.raise_for_status()
    data = r.json()
    if isinstance(data, dict):
        keys = list(data.keys())[:10]
        print("json keys (top 10):", keys)
    print("sample:", str(data)[:400])


if __name__ == "__main__":
    check("https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum&vs_currencies=usd,eur")
    check("https://api.frankfurter.dev/v1/latest?base=EUR&symbols=USD,GBP")

    if not api_key:
        raise SystemExit("TWELVEDATA_API_KEY manquant (vérifie le fichier .env à la racine).")

    check(f"https://api.twelvedata.com/time_series?symbol=AAPL&interval=1day&outputsize=30&apikey={api_key}")
