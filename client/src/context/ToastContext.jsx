import { createContext, useContext, useState, useCallback } from 'react';

const ToastContext = createContext();

export const useToast = () => {
  const context = useContext(ToastContext);
  if (!context) {
    throw new Error('useToast must be used within ToastProvider');
  }
  return context;
};

let toastId = 0;

export const ToastProvider = ({ children }) => {
  const [toasts, setToasts] = useState([]);

  const addToast = useCallback(
    ({ type = 'info', title, message, duration = 5000, action }) => {
      const id = toastId++;
      const toast = { id, type, title, message, action };

      setToasts((prev) => [...prev, toast]);

      if (duration > 0) {
        setTimeout(() => {
          removeToast(id);
        }, duration);
      }

      return id;
    },
    []
  );

  const removeToast = useCallback((id) => {
    setToasts((prev) => prev.filter((toast) => toast.id !== id));
  }, []);

  // Convenience methods
  const success = useCallback(
    (title, message, options = {}) => {
      return addToast({ type: 'success', title, message, ...options });
    },
    [addToast]
  );

  const error = useCallback(
    (title, message, options = {}) => {
      return addToast({ type: 'error', title, message, ...options });
    },
    [addToast]
  );

  const warning = useCallback(
    (title, message, options = {}) => {
      return addToast({ type: 'warning', title, message, ...options });
    },
    [addToast]
  );

  const info = useCallback(
    (title, message, options = {}) => {
      return addToast({ type: 'info', title, message, ...options });
    },
    [addToast]
  );

  const value = {
    toasts,
    addToast,
    removeToast,
    success,
    error,
    warning,
    info,
  };

  return <ToastContext.Provider value={value}>{children}</ToastContext.Provider>;
};
