import { useState, useCallback, memo } from 'react';
import { Copy, Check } from 'lucide-react';

function CopyButton({ value, variant = 'light' }) {
  const [copied, setCopied] = useState(false);

  const handleCopy = useCallback((e) => {
    e.stopPropagation();
    navigator.clipboard.writeText(value).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    });
  }, [value]);

  return (
    <button
      className={`copy-btn copy-btn--${variant}`}
      onClick={handleCopy}
      title={`Copy: ${value}`}
    >
      {copied ? <Check size={11} /> : <Copy size={11} />}
    </button>
  );
}

export default memo(CopyButton);
