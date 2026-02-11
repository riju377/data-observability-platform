import { createContext, useContext, useEffect, useState } from 'react';

const ThemeContext = createContext();

export const useTheme = () => {
  const context = useContext(ThemeContext);
  if (!context) {
    throw new Error('useTheme must be used within ThemeProvider');
  }
  return context;
};

export const ThemeProvider = ({ children }) => {
  // Get initial theme from localStorage or default to 'system'
  const [theme, setTheme] = useState(() => {
    const savedTheme = localStorage.getItem('theme');
    return savedTheme || 'system';
  });

  // Get the actual theme to apply (resolves 'system' to 'light' or 'dark')
  const getResolvedTheme = () => {
    if (theme === 'system') {
      return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
    }
    return theme;
  };

  const [resolvedTheme, setResolvedTheme] = useState(getResolvedTheme());

  // Update document theme
  useEffect(() => {
    const root = document.documentElement;
    const resolved = getResolvedTheme();

    // Remove previous theme
    root.removeAttribute('data-theme');

    // Set new theme
    root.setAttribute('data-theme', resolved);
    setResolvedTheme(resolved);

    // Save to localStorage
    localStorage.setItem('theme', theme);

    // Listen for system theme changes if theme is 'system'
    if (theme === 'system') {
      const mediaQuery = window.matchMedia('(prefers-color-scheme: dark)');
      const handleChange = (e) => {
        const newResolved = e.matches ? 'dark' : 'light';
        root.setAttribute('data-theme', newResolved);
        setResolvedTheme(newResolved);
      };

      mediaQuery.addEventListener('change', handleChange);
      return () => mediaQuery.removeEventListener('change', handleChange);
    }
  }, [theme]);

  const toggleTheme = () => {
    setTheme((prev) => {
      if (prev === 'light') return 'dark';
      if (prev === 'dark') return 'system';
      return 'light';
    });
  };

  const setThemeMode = (mode) => {
    if (['light', 'dark', 'system'].includes(mode)) {
      setTheme(mode);
    }
  };

  const value = {
    theme, // Current theme setting ('light' | 'dark' | 'system')
    resolvedTheme, // Actual applied theme ('light' | 'dark')
    toggleTheme,
    setTheme: setThemeMode,
  };

  return <ThemeContext.Provider value={value}>{children}</ThemeContext.Provider>;
};
