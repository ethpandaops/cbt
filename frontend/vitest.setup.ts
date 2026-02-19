import '@testing-library/jest-dom/vitest';
import { beforeAll, vi } from 'vitest';

// Suppress console warnings during tests to keep output clean
beforeAll(() => {
  const originalWarn = console.warn;
  vi.spyOn(console, 'warn').mockImplementation((...args) => {
    const message = args[0]?.toString() || '';
    if (message.includes('Failed to parse') || message.includes('Validation error')) {
      return;
    }
    originalWarn(...args);
  });

  // Mock DOM dimensions (jsdom doesn't compute actual dimensions)
  Object.defineProperty(HTMLElement.prototype, 'clientWidth', {
    configurable: true,
    value: 800,
  });

  Object.defineProperty(HTMLElement.prototype, 'clientHeight', {
    configurable: true,
    value: 600,
  });

  // Mock ResizeObserver
  const ResizeObserverMock = vi.fn(function (this: ResizeObserver) {
    this.observe = vi.fn();
    this.unobserve = vi.fn();
    this.disconnect = vi.fn();
  });

  vi.stubGlobal('ResizeObserver', ResizeObserverMock);
});
