import React from 'react';

export function PressureGauge({ score = 0, level = 'LOW', eps = 0, dropped = 0 }) {
  const percentage = Math.min(100, Math.max(0, Math.round(score * 100)));
  
  let strokeColor = 'var(--accent-green)';
  if (level === 'MEDIUM') strokeColor = 'var(--accent-yellow)';
  if (level === 'HIGH') strokeColor = 'rgba(249, 115, 22, 1)';
  if (level === 'CRITICAL') strokeColor = 'var(--accent-red)';

  const radius = 60;
  const circumference = 2 * Math.PI * radius;
  const strokeDashoffset = circumference - (percentage / 100) * circumference;

  return (
    <div className="glass-panel" style={{ padding: '20px', display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center' }}>
      <h3 style={{ fontSize: '14px', fontWeight: '700', color: 'var(--text-secondary)', marginBottom: '16px' }}>
        SYSTEM PRESSURE GAUGE
      </h3>

      <div style={{ position: 'relative', width: '160px', height: '160px', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
        <svg width="160" height="160" style={{ transform: 'rotate(-90deg)' }}>
          <circle
            cx="80"
            cy="80"
            r={radius}
            stroke="rgba(255, 255, 255, 0.05)"
            strokeWidth="12"
            fill="transparent"
          />
          <circle
            cx="80"
            cy="80"
            r={radius}
            stroke={strokeColor}
            strokeWidth="12"
            fill="transparent"
            strokeDasharray={circumference}
            strokeDashoffset={strokeDashoffset}
            strokeLinecap="round"
            style={{ transition: 'stroke-dashoffset 0.5s ease, stroke 0.5s ease' }}
          />
        </svg>

        <div style={{ position: 'absolute', textAlign: 'center' }}>
          <div style={{ fontSize: '28px', fontWeight: '800', fontFamily: 'var(--font-mono)' }}>
            {percentage}%
          </div>
          <div style={{ fontSize: '12px', fontWeight: '700', color: strokeColor }}>
            {level}
          </div>
        </div>
      </div>

      <div style={{ marginTop: '20px', display: 'flex', width: '100%', justifyContent: 'space-around', borderTop: '1px solid var(--bg-card-border)', paddingTop: '12px' }}>
        <div style={{ textAlign: 'center' }}>
          <div style={{ fontSize: '11px', color: 'var(--text-muted)' }}>Throughput</div>
          <div style={{ fontSize: '15px', fontWeight: '700', fontFamily: 'var(--font-mono)', color: 'var(--accent-cyan)' }}>
            {eps} /s
          </div>
        </div>

        <div style={{ textAlign: 'center' }}>
          <div style={{ fontSize: '11px', color: 'var(--text-muted)' }}>Total Shed</div>
          <div style={{ fontSize: '15px', fontWeight: '700', fontFamily: 'var(--font-mono)', color: dropped > 0 ? 'var(--accent-red)' : 'var(--text-secondary)' }}>
            {dropped}
          </div>
        </div>
      </div>
    </div>
  );
}
