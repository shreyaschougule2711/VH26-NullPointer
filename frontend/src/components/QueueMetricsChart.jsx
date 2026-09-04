import React from 'react';
import { Layers } from 'lucide-react';

export function QueueMetricsChart({ queues = {} }) {
  const queueData = [
    { name: 'Raw Queue', count: queues.raw || 0, color: 'var(--accent-cyan)' },
    { name: 'Critical Queue', count: queues.critical || 0, color: 'var(--accent-green)' },
    { name: 'Batch Queue', count: queues.batch || 0, color: 'var(--accent-purple)' },
    { name: 'Deferred Queue', count: queues.deferred || 0, color: 'var(--accent-yellow)' },
    { name: 'DLQ / Shed Audit', count: queues.dlq || 0, color: 'var(--accent-red)' },
  ];

  const maxVal = Math.max(10, ...queueData.map(q => q.count));

  return (
    <div className="glass-panel" style={{ padding: '20px' }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '16px' }}>
        <Layers size={18} color="var(--accent-cyan)" />
        <h3 style={{ fontSize: '15px', fontWeight: '700' }}>Kafka Topic Queue Occupancy</h3>
      </div>

      <div style={{ display: 'flex', flexDirection: 'column', gap: '14px' }}>
        {queueData.map((q) => {
          const widthPct = Math.min(100, Math.round((q.count / maxVal) * 100));
          return (
            <div key={q.name}>
              <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '13px', marginBottom: '4px' }}>
                <span style={{ fontWeight: '500' }}>{q.name}</span>
                <span style={{ fontFamily: 'var(--font-mono)', fontWeight: '700', color: q.color }}>
                  {q.count} msgs
                </span>
              </div>
              <div style={{ width: '100%', height: '8px', background: 'rgba(255, 255, 255, 0.05)', borderRadius: '4px', overflow: 'hidden' }}>
                <div
                  style={{
                    width: `${widthPct}%`,
                    height: '100%',
                    background: q.color,
                    borderRadius: '4px',
                    transition: 'width 0.4s ease'
                  }}
                />
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
