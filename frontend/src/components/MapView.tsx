import { useEffect, useRef } from 'react';
import * as maplibregl from 'maplibre-gl';
import 'maplibre-gl/dist/maplibre-gl.css';

interface Props {
  centerLat?: number;
  centerLng?: number;
}

export default function MapView({ centerLat = 52.52, centerLng = 13.405 }: Props) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const mapRef = useRef<maplibregl.Map | null>(null);

  useEffect(() => {
    if (!containerRef.current || mapRef.current) return;

    // Create map with free OpenFreeMap style (no token!)
    mapRef.current = new maplibregl.Map({
      container: containerRef.current,
      style: 'https://tiles.openfreemap.org/styles/positron', // or 'liberty' / 'refuge'
      center: [centerLng, centerLat],
      zoom: 12,
    });

    mapRef.current.addControl(new maplibregl.NavigationControl(), 'top-left');

    // Example: add heatmap layer (uncomment when you have data)
    // mapRef.current.on('load', () => {
    //   mapRef.current!.addSource('traffic', {
    //     type: 'geojson',
    //     data: { type: 'FeatureCollection', features: [] }
    //   });
    //   mapRef.current!.addLayer({
    //     id: 'traffic-heat',
    //     type: 'heatmap',
    //     source: 'traffic',
    //     paint: {
    //       'heatmap-weight': 0.6,
    //       'heatmap-intensity': 0.8,
    //       'heatmap-color': [
    //         'interpolate',
    //         ['linear'],
    //         ['heatmap-density'],
    //         0, 'rgba(0,0,255,0)',
    //         0.2, 'rgb(0,0,255)',
    //         0.4, 'rgb(0,255,255)',
    //         0.6, 'rgb(0,255,0)',
    //         0.8, 'rgb(255,255,0)',
    //         1, 'rgb(255,0,0)'
    //       ]
    //     }
    //   });
    // });

    // Example: add markers (using DOM elements)
    // mapRef.current.on('load', () => {
    //   const el = document.createElement('div');
    //   el.className = 'vehicle-marker';
    //   new maplibregl.Marker(el)
    //     .setLngLat([13.405, 52.52])
    //     .addTo(mapRef.current!);
    // });

    return () => {
      mapRef.current?.remove();
      mapRef.current = null;
    };
  }, [centerLat, centerLng]);

  // No token check needed – always render the map
  return <div ref={containerRef} style={{ width: '100%', height: '100%' }} />;
}
