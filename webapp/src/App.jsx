import React, { useEffect, useState } from 'react'
import { fetchReturns, fetchVol, fetchRecommendation, fetchSymbols, fetchPredictions } from './api'
import Chart from 'chart.js/auto'

function useChart(canvasId, label, color) {
  const [chart, setChart] = useState(null)
  useEffect(() => {
    const ctx = document.getElementById(canvasId)
    if (!ctx) return
    const c = new Chart(ctx, {
      type: 'line',
      data: { labels: [], datasets: [{ label, data: [], borderColor: color, fill: false }] },
      options: { responsive: true, scales: { x: { ticks: { maxTicksLimit: 8 } } } },
    })
    setChart(c)
    return () => c.destroy()
  }, [canvasId, label, color])
  return chart
}

export default function App() {
  const [assetType, setAssetType] = useState('stock')
  const [symbol, setSymbol] = useState('')
  const [symbols, setSymbols] = useState([])
  const [rec, setRec] = useState(null)
  const [preds, setPreds] = useState([])
  const [predAt, setPredAt] = useState('')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')

  const returnsChart = useChart('returnsChart', 'Close', '#1d4ed8')
  const volChart = useChart('volChart', 'Volatility 30d', '#dc2626')

  const load = async () => {
    setLoading(true)
    setError('')
    try {
      const [rets, vols, recResp, predResp] = await Promise.all([
        fetchReturns(assetType, symbol, 180),
        fetchVol(assetType, symbol, 180),
        fetchRecommendation(assetType, symbol),
        fetchPredictions(assetType, symbol),
      ])
      if (returnsChart) {
        returnsChart.data.labels = rets.map((r) => r.date).reverse()
        returnsChart.data.datasets[0].data = rets.map((r) => r.close).reverse()
        returnsChart.update()
      }
      if (volChart) {
        volChart.data.labels = vols.map((v) => v.date).reverse()
        volChart.data.datasets[0].data = vols.map((v) => v.volatility).reverse()
        volChart.update()
      }
      setRec(recResp)
      setPreds(predResp?.predictions || [])
      setPredAt(predResp?.generated_at || '')
    } catch (e) {
      setError(e.message)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    const t = setTimeout(() => load(), 300)
    return () => clearTimeout(t)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [assetType, symbol, returnsChart, volChart])

  // load symbols when assetType changes
  useEffect(() => {
    fetchSymbols(assetType)
      .then((list) => {
        setSymbols(list)
        if (list.length && !symbol) setSymbol(list[0].symbol)
        if (list.length && symbol && !list.find((s) => s.symbol === symbol)) setSymbol(list[0].symbol)
      })
      .catch((e) => setError(e.message))
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [assetType])

  return (
    <div className="page">
      <header>
        <h1>Market Lake Gold Viewer</h1>
        <p>Visualise Gold returns/volatility plus rule-based and ML signals.</p>
      </header>

      <section className="controls">
        <label>
          Asset type
          <select value={assetType} onChange={(e) => setAssetType(e.target.value)}>
            <option value="stock">stock</option>
            <option value="crypto">crypto</option>
            <option value="forex">forex</option>
          </select>
        </label>
        <label>
          Symbol
          <select value={symbol} onChange={(e) => setSymbol(e.target.value)}>
            {symbols.map((s) => (
              <option key={`${s.asset_type}-${s.symbol}`} value={s.symbol}>
                {s.symbol}
              </option>
            ))}
          </select>
        </label>
        <button onClick={load} disabled={loading}>
          {loading ? 'Loading...' : 'Refresh'}
        </button>
      </section>

      {error && <div className="error">{error}</div>}

      <section className="charts">
        <div className="card">
          <canvas id="returnsChart" height="120" />
        </div>
        <div className="card">
          <canvas id="volChart" height="120" />
        </div>
      </section>

      <section className="rec">
        <h3>Recommendation</h3>
        {rec ? (
          <div className={`badge ${rec.action}`}>
            {rec.action.toUpperCase()} - slope: {rec.slope?.toFixed(5)} | last close: {rec.last_close?.toFixed(2)}
          </div>
        ) : (
          <div>Waiting for data...</div>
        )}
      </section>

      <section className="predictions card">
        <div className="pred-header">
          <h3>ML Prediction (lasso)</h3>
          <span className="muted">{predAt ? `generated ${predAt}` : 'no run yet'}</span>
        </div>
        {preds.length ? (
          <table>
            <thead>
              <tr>
                <th>Symbol</th>
                <th>Action</th>
                <th>Pred 1d return</th>
                <th>Samples</th>
              </tr>
            </thead>
            <tbody>
              {preds.slice(0, 5).map((p) => (
                <tr key={`${p.asset_type}-${p.symbol}`}>
                  <td>{p.symbol}</td>
                  <td>
                    <span className={`badge ${p.action}`}>{p.action}</span>
                  </td>
                  <td>{p.prediction_return?.toFixed(4)}</td>
                  <td>{p.samples}</td>
                </tr>
              ))}
            </tbody>
          </table>
        ) : (
          <div className="muted">No prediction for this asset yet. Run ml_predict.py.</div>
        )}
      </section>
    </div>
  )
}
