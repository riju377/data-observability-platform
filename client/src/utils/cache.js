/**
 * Simple in-memory cache with TTL and request deduplication
 */
class Cache {
  constructor() {
    this.store = new Map();
    this.inflight = new Map();
  }

  get(key) {
    const entry = this.store.get(key);
    if (!entry) return null;

    const { value, timestamp, ttl } = entry;
    if (Date.now() - timestamp > ttl) {
      this.store.delete(key);
      return null;
    }
    return value;
  }

  set(key, value, ttl = 60000) {
    this.store.set(key, {
      value,
      timestamp: Date.now(),
      ttl
    });
  }

  has(key) {
    return this.get(key) !== null;
  }

  delete(key) {
    this.store.delete(key);
  }

  clear() {
    this.store.clear();
    this.inflight.clear();
  }

  /**
   * Get cached value or fetch with deduplication
   * If request already in-flight, return same promise
   */
  async getOrFetch(key, fetcher, ttl = 60000) {
    // Check cache first
    const cached = this.get(key);
    if (cached !== null) return cached;

    // Check if already fetching
    if (this.inflight.has(key)) {
      return this.inflight.get(key);
    }

    // Start new fetch
    const promise = fetcher()
      .then(result => {
        this.set(key, result, ttl);
        this.inflight.delete(key);
        return result;
      })
      .catch(error => {
        this.inflight.delete(key);
        throw error;
      });

    this.inflight.set(key, promise);
    return promise;
  }

  /**
   * Invalidate keys matching pattern
   */
  invalidate(pattern) {
    const regex = new RegExp(pattern);
    for (const key of this.store.keys()) {
      if (regex.test(key)) {
        this.store.delete(key);
      }
    }
  }
}

export const apiCache = new Cache();

export const getCacheKey = (endpoint, params = {}) => {
  const sortedParams = Object.keys(params)
    .sort()
    .reduce((acc, key) => {
      acc[key] = params[key];
      return acc;
    }, {});

  return `${endpoint}:${JSON.stringify(sortedParams)}`;
};
