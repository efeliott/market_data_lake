const BASE = '/api'

export async function fetchReturns(assetType, symbol, limit = 180) {
  const url = `${BASE}/returns?asset_type=${assetType}&symbol=${symbol}&limit=${limit}`
  const r = await fetch(url)
  if (!r.ok) throw new Error('API returns failed')
  return r.json()
}

export async function fetchVol(assetType, symbol, limit = 180) {
  const url = `${BASE}/volatility?asset_type=${assetType}&symbol=${symbol}&limit=${limit}`
  const r = await fetch(url)
  if (!r.ok) throw new Error('API volatility failed')
  return r.json()
}

export async function fetchRecommendation(assetType, symbol) {
  const url = `${BASE}/recommendation?asset_type=${assetType}&symbol=${symbol}`
  const r = await fetch(url)
  if (!r.ok) throw new Error('API recommendation failed')
  return r.json()
}

export async function fetchSymbols(assetType) {
  const url = assetType ? `${BASE}/symbols?asset_type=${assetType}` : `${BASE}/symbols`
  const r = await fetch(url)
  if (!r.ok) throw new Error('API symbols failed')
  return r.json()
}
