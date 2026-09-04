import React from 'react';
import { Clock } from 'lucide-react';

export function LatencyChart({ avgLatencyMs = 0, history = [] }) {
  // SVG sparkline rendering for execution latency history
  const maxLat = Math.max(50, ...history, avgLatencyMs);
  const height = 100;
  const width = 300;

  const points = history.length > 1
    ? history.map((val, i) => {
        const x = (i / (history.length - 1)) * width;
        const y = height - (val / maxLat) * height;
        return `${x},${y}`;
      }).join(' ')
    : `0,${height} ${width},${height}`;

  return (
    <div className="glass-panel" style={{ padding: '20px' }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '12px' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
          <Clock size={18} color="var(--accent-purple)" />
          <h3 style={{ fontSize: '15px', fontWeight: '700' }}>Average Latency (ms)</h3>
        </div>
        <div style={{ fontSize: '20px', fontWeight: '800', fontFamily: 'var(--font-mono)', color: 'var(--accent-purple)' }}>
          {avgLatencyMs} ms
        </div>
      </div>

      <div style={{ width: '100%', height: '100px', display: 'flex', alignItems: 'center', justifyContent: 'center', background: 'rgba(0,0,0,0.2)', borderRadius: '8px', padding: '8px' }}>
        <svg width="100%" height="100%" viewBox={`0 0 ${width} ${height}`} preserveAspectRatio="none">
          <polyline
            fill="none"
            stroke="var(--accent-purple)"
            strokeWidth="3"
            points={points}
            strokeLinecap="round"
          />
        </svg>
      </div>
    </div>
  );
}
