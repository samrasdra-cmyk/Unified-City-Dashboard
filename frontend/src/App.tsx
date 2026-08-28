import { useEffect, useState } from "react";
import KPICard from "./components/KPICard";
import MapView from "./components/MapView";
import TrendChart from "./components/TrendChart";
import SimulationSlider from "./components/SimulationSlider";
import { useWebSocket } from "./hooks/useWebSocket";
import { apiClient } from "./services/apiClient";
import type { ConnectorStatus, DashboardSnapshot } from "./types";

function fmt(value: number | null | undefined, suffix = ""): string {
  if (value === null || value === undefined) return "—";
  return `${value}${suffix}`;
}

export default function App() {
  const { data: liveSnapshot, connected } = useWebSocket<DashboardSnapshot>();
  const [snapshot, setSnapshot] = useState<DashboardSnapshot | null>(null);
  const [connectors, setConnectors] = useState<ConnectorStatus[]>([]);

  useEffect(() => {
    apiClient.getLatestSnapshot().then((s) => setSnapshot(s as DashboardSnapshot)).catch(() => {});
  }, []);

  useEffect(() => {
    if (liveSnapshot) setSnapshot(liveSnapshot);
  }, [liveSnapshot]);

  useEffect(() => {
    const load = () =>
      apiClient
        .getConnectorStatus()
        .then((c) => setConnectors(c as ConnectorStatus[]))
        .catch(() => {});
    load();
    const interval = setInterval(load, 15000);
    return () => clearInterval(interval);
  }, []);

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div>
          <h1>Unified City Dashboard</h1>
          <div className="subtitle">Real-time urban operations</div>
        </div>

        <div className="kpi-grid">
          <KPICard label="Avg Speed" value={fmt(snapshot?.avg_speed_kmh, " km/h")} />
          <KPICard label="Avg AQI" value={fmt(snapshot?.avg_aqi)} />
          <KPICard label="Transit On-Time" value={fmt(snapshot?.transit_on_time_pct, "%")} />
          <KPICard label="Avg Waste Fill" value={fmt(snapshot?.avg_waste_fill_pct, "%")} />
          <KPICard label="Thermal Comfort" value={fmt(snapshot?.thermal_comfort, "°C")} />
          <KPICard label="Feels Like" value={fmt(snapshot?.feels_like, "°C")} />
        </div>

        <div>
          <h2 style={{ fontSize: 13, color: "#8b98a5", margin: "8px 0" }}>
            Connector Health
          </h2>
          {connectors.length === 0 && (
            <div style={{ fontSize: 12, color: "#8b98a5" }}>Loading connector status…</div>
          )}
          {connectors.map((c) => (
            <div className="status-row" key={c.name}>
              <span className={`status-dot ${c.status}`} />
              <span style={{ textTransform: "capitalize" }}>{c.name.replace("_", " ")}</span>
              {c.using_fallback && (
                <span style={{ marginLeft: "auto", fontSize: 11, color: "#8b98a5" }}>
                  simulator
                </span>
              )}
            </div>
          ))}
        </div>

        <div>
          <h2 style={{ fontSize: 13, color: "#8b98a5", margin: "8px 0" }}>
            3D Digital Twin Stub
          </h2>
          <SimulationSlider />
        </div>
      </aside>

      <main className="main-area">
        <div className="map-container">
          <div className="ws-indicator">
            <span
              className="status-dot"
              style={{ background: connected ? "#3fb950" : "#f85149" }}
            />
            {connected ? "Live" : "Reconnecting…"}
          </div>
          <MapView />
        </div>
        <div className="chart-panel">
          <TrendChart metric="traffic" title="Avg Speed (last 6h)" />
        </div>
      </main>
    </div>
  );
}
