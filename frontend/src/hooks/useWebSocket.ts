import { useEffect, useRef, useState } from "react";

const WS_URL = import.meta.env.VITE_WS_URL || "ws://localhost:8000/ws/live";
const RECONNECT_DELAY_MS = 3000;

export function useWebSocket<T>() {
  const [data, setData] = useState<T | null>(null);
  const [connected, setConnected] = useState(false);
  const wsRef = useRef<WebSocket | null>(null);
  const reconnectTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const mounted = useRef(true);

  useEffect(() => {
    mounted.current = true;
    let disposed = false;

    const connect = () => {
      const ws = new WebSocket(WS_URL);
      wsRef.current = ws;

      ws.onopen = () => {
        // React Strict Mode mounts, cleans up, and mounts once more in
        // development. Do not close a socket while it is still connecting:
        // browsers report that as a failed connection.
        if (disposed) {
          ws.close();
          return;
        }
        if (mounted.current) setConnected(true);
      };

      ws.onmessage = (event) => {
        try {
          const parsed = JSON.parse(event.data);
          if (mounted.current) setData(parsed);
        } catch {
          // ignore malformed frames
        }
      };

      ws.onclose = () => {
        if (mounted.current) {
          setConnected(false);
          reconnectTimer.current = setTimeout(connect, RECONNECT_DELAY_MS);
        }
      };

      ws.onerror = () => {
        ws.close();
      };
    };

    connect();

    return () => {
      disposed = true;
      mounted.current = false;
      if (reconnectTimer.current) clearTimeout(reconnectTimer.current);
      if (wsRef.current?.readyState === WebSocket.OPEN) wsRef.current.close();
    };
  }, []);

  return { data, connected };
}
