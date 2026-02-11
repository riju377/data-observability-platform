import { Loader2 } from 'lucide-react';
import './LoadingSpinner.css';

export const LoadingSpinner = ({ size = 48, message = 'Loading...' }) => {
  return (
    <div className="loading-spinner-container">
      <Loader2 size={size} className="loading-spinner-icon" />
      {message && <p className="loading-spinner-message">{message}</p>}
    </div>
  );
};

export default LoadingSpinner;
