import { useEffect, useRef, useState } from 'react';
import * as maplibregl from 'maplibre-gl';
import 'maplibre-gl/dist/maplibre-gl.css';
import { apiClient } from '../services/apiClient';
import type { GeoJSONFeatureCollection } from '../types';

interface Props {
  centerLat?: number;
  centerLng?: number;
}

export default function MapView({ centerLat = 52.52, centerLng = 13.405 }: Props) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const mapRef = useRef<maplibregl.Map | null>(null);
  const [activeLayer, setActiveLayer] = useState<'temperature' | 'traffic' | 'air'>('temperature');

  useEffect(() => {
    if (!containerRef.current) return;

    const map = new maplibregl.Map({
      container: containerRef.current,
      style: {
        version: 8,
        sources: {
          'osm-raster': {
            type: 'raster',
            tiles: [
              'https://a.tile.openstreetmap.org/{z}/{x}/{y}.png',
              'https://b.tile.openstreetmap.org/{z}/{x}/{y}.png',
              'https://c.tile.openstreetmap.org/{z}/{x}/{y}.png',
            ],
            tileSize: 256,
            attribution:
              '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
          },
        },
        layers: [
          {
            id: 'osm-raster-layer',
            type: 'raster',
            source: 'osm-raster',
            minzoom: 0,
            maxzoom: 19,
          },
        ],
      },
      center: [centerLng, centerLat],
      zoom: 12,
    });

    map.addControl(new maplibregl.NavigationControl(), 'top-left');
    mapRef.current = map;

    map.on('load', () => {
      // Temperature Heatmap Source
      map.addSource('temperature', {
        type: 'geojson',
        data: {
          type: 'FeatureCollection',
          features: [],
        },
      });

      map.addLayer({
        id: 'temperature-heat',
        type: 'heatmap',
        source: 'temperature',
        paint: {
          'heatmap-weight': ['interpolate', ['linear'], ['get', 'thermal_comfort'], 20, 0.2, 35, 1],
          'heatmap-intensity': 0.8,
          'heatmap-radius': 30,
          'heatmap-opacity': 0.75,
          'heatmap-color': [
            'interpolate',
            ['linear'],
            ['heatmap-density'],
            0, 'rgba(0, 0, 255, 0)',
            0.2, 'rgba(0, 255, 255, 0.5)',
            0.4, 'rgba(0, 255, 0, 0.6)',
            0.6, 'rgba(255, 255, 0, 0.7)',
            0.8, 'rgba(255, 165, 0, 0.8)',
            1, 'rgba(255, 0, 0, 0.9)',
          ],
        },
      });
    });

    setTimeout(() => map.resize(), 100);

    const resizeObserver = new ResizeObserver(() => {
      map.resize();
    });
    resizeObserver.observe(containerRef.current);

    return () => {
      resizeObserver.disconnect();
      map.remove();
      mapRef.current = null;
    };
  }, [centerLat, centerLng]);

  // Periodically fetch heatmap data based on activeLayer
  useEffect(() => {
    let isMounted = true;
    const fetchHeatmap = async () => {
      try {
        const geojson = await apiClient.getHeatmap(activeLayer) as GeoJSONFeatureCollection;
        if (!isMounted || !mapRef.current) return;
        const source = mapRef.current.getSource(activeLayer) as maplibregl.GeoJSONSource | undefined;
        if (source) {
          source.setData(geojson);
        }
      } catch (err) {
        // Silently handle if backend is starting up
      }
    };

    fetchHeatmap();
    const interval = setInterval(fetchHeatmap, 10000);
    return () => {
      isMounted = false;
      clearInterval(interval);
    };
  }, [activeLayer]);

  return (
    <div style={{ width: '100%', height: '100%', position: 'relative' }}>
      <div
        style={{
          position: 'absolute',
          top: 12,
          left: 60,
          zIndex: 10,
          background: 'rgba(18, 24, 32, 0.85)',
          border: '1px solid #1f2833',
          borderRadius: 8,
          padding: '4px 8px',
          display: 'flex',
          gap: 6,
          fontSize: 12,
        }}
      >
        <button
          type="button"
          onClick={() => setActiveLayer('temperature')}
          style={{
            background: activeLayer === 'temperature' ? '#3ea6ff' : 'transparent',
            color: '#fff',
            border: 'none',
            borderRadius: 4,
            padding: '4px 8px',
            cursor: 'pointer',
            fontWeight: activeLayer === 'temperature' ? 600 : 400,
          }}
        >
          🌡️ FortyGuard Thermal
        </button>
      </div>
      <div
        ref={containerRef}
        style={{
          width: '100%',
          height: '100%',
          minHeight: '400px',
          position: 'relative',
        }}
      />
    </div>
  );
}