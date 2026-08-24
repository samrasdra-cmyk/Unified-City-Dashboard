import { useEffect, useState } from "react";
import ReactECharts from "echarts-for-react";
import { apiClient } from "../services/apiClient";
import type { HistoryPoint } from "../types";

interface Props {
  metric: "aqi" | "traffic";
  title: string;
}

export default function TrendChart({ metric, title }: Props) {
  const [points, setPoints] = useState<HistoryPoint[]>([]);

  useEffect(() => {
    let cancelled = false;

    const load = async () => {
      try {
        const data = (await apiClient.getHistory(metric, 6)) as HistoryPoint[];
        if (!cancelled) setPoints(data);
      } catch {
        // silently keep last known points on transient errors
      }
    };

    load();
    const interval = setInterval(load, 30000);
    return () => {
      cancelled = true;
      clearInterval(interval);
    };
  }, [metric]);

  const option = {
    backgroundColor: "transparent",
    textStyle: { color: "#e6edf3" },
    title: { text: title, textStyle: { color: "#e6edf3", fontSize: 13 }, top: 0, left: 0 },
    grid: { left: 40, right: 16, top: 32, bottom: 24 },
    xAxis: {
      type: "category",
      data: points.map((p) => p.timestamp.slice(11, 16)),
      axisLine: { lineStyle: { color: "#1f2833" } },
      axisLabel: { color: "#8b98a5" },
    },
    yAxis: {
      type: "value",
      axisLine: { lineStyle: { color: "#1f2833" } },
      splitLine: { lineStyle: { color: "#1f2833" } },
      axisLabel: { color: "#8b98a5" },
    },
    series: [
      {
        data: points.map((p) => p.value),
        type: "line",
        smooth: true,
        showSymbol: false,
        lineStyle: { color: "#3ea6ff" },
        areaStyle: { color: "rgba(62,166,255,0.15)" },
      },
    ],
  };

  return <ReactECharts option={option} style={{ height: "100%", width: "100%" }} />;
}
