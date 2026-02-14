
import React from 'react';
import { ArrowUp, ArrowDown, Activity, Minus } from 'lucide-react';
import './AnomalyMetricComparison.css';

const AnomalyMetricComparison = ({ actual, expected, type, severity }) => {
    if (actual === undefined || expected === undefined) return null;

    const actualVal = typeof actual === 'object' ? actual.value : actual;
    const expectedVal = typeof expected === 'object' ? expected.value : expected;

    const diff = actualVal - expectedVal;
    const percentChange = expectedVal !== 0 ? (diff / expectedVal) * 100 : 0;
    const isSpike = diff > 0;
    const isDrop = diff < 0;

    // Determine color based on type and direction
    // For RowCount: Drop is usually bad (Red), Spike might be Warning (Orange)
    // For Volume: Similar logic
    let colorClass = 'neutral';
    if (severity === 'CRITICAL') colorClass = 'critical';
    else if (severity === 'WARNING') colorClass = 'warning';

    const formattedActual = typeof actualVal === 'number' ? actualVal.toLocaleString() : actualVal;
    const formattedExpected = typeof expectedVal === 'number' ? expectedVal.toLocaleString() : expectedVal;

    return (
        <div className={`metric-comparison ${colorClass}`}>
            <div className="metric-box actual-box">
                <span className="metric-label">Actual</span>
                <div className="metric-value-row">
                    <span className="metric-value">{formattedActual}</span>
                    <span className={`trend-indicator ${isSpike ? 'spike' : 'drop'}`}>
                        {isSpike && <ArrowUp size={14} />}
                        {isDrop && <ArrowDown size={14} />}
                        {!isSpike && !isDrop && <Minus size={14} />}
                        <span>{Math.abs(percentChange).toFixed(1)}%</span>
                        <span className="deviation-text">deviation</span>
                    </span>
                </div>
            </div>

            <div className="metric-divider">
                <Activity size={16} />
            </div>

            <div className="metric-box expected-box">
                <span className="metric-label">Expected</span>
                <span className="metric-value">{formattedExpected}</span>
                <span className="metric-subtext">Based on 30-day avg</span>
            </div>
        </div>
    );
};

export default AnomalyMetricComparison;
