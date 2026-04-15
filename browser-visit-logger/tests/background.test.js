'use strict';
/**
 * Unit tests for extension/background.js
 *
 * Run with:
 *   cd browser-visit-logger
 *   npm test
 *
 * The Chrome extension APIs (chrome.webNavigation, chrome.tabs,
 * chrome.runtime) are mocked below — no browser required.
 */

// ---------------------------------------------------------------------------
// Chrome API mocks (declared once; cleared/reset before each test)
// ---------------------------------------------------------------------------
const mockSendNativeMessage = jest.fn();
const mockTabsGet           = jest.fn();
const addNavListener        = jest.fn();
const addTabUpdateListener  = jest.fn();

function buildChromeMock() {
  global.chrome = {
    runtime: {
      sendNativeMessage: mockSendNativeMessage,
      lastError: null,
    },
    webNavigation: {
      onCompleted: { addListener: addNavListener },
    },
    tabs: {
      get:       mockTabsGet,
      onUpdated: { addListener: addTabUpdateListener },
    },
  };
}

// ---------------------------------------------------------------------------
// Load a fresh copy of background.js before every test
// (jest.resetModules() clears the module cache so pendingVisits starts empty)
// ---------------------------------------------------------------------------
let navHandler, tabUpdateHandler;

beforeEach(() => {
  jest.useFakeTimers();

  // Clear mock state from previous test
  mockSendNativeMessage.mockClear();
  mockTabsGet.mockClear();
  addNavListener.mockClear();
  addTabUpdateListener.mockClear();

  // Fresh chrome mock
  buildChromeMock();

  // Fresh module (new pendingVisits Map, fresh listener registrations)
  jest.resetModules();
  require('../extension/background');

  navHandler       = addNavListener.mock.calls[0][0];
  tabUpdateHandler = addTabUpdateListener.mock.calls[0][0];
});

afterEach(() => {
  jest.useRealTimers();
});

// ---------------------------------------------------------------------------
// Helper: simulate tabs.get returning a tab object
// ---------------------------------------------------------------------------
function tabReturns(title) {
  mockTabsGet.mockImplementation((_tabId, cb) => cb({ title }));
}

function tabReturnsError(message) {
  mockTabsGet.mockImplementation((_tabId, cb) => {
    global.chrome.runtime.lastError = { message };
    cb(null);
    global.chrome.runtime.lastError = null;
  });
}

// ---------------------------------------------------------------------------
// Listener registration
// ---------------------------------------------------------------------------
describe('listener registration', () => {
  test('registers a webNavigation.onCompleted listener', () => {
    expect(addNavListener).toHaveBeenCalledTimes(1);
    expect(typeof navHandler).toBe('function');
  });

  test('registers a tabs.onUpdated listener', () => {
    expect(addTabUpdateListener).toHaveBeenCalledTimes(1);
    expect(typeof tabUpdateHandler).toBe('function');
  });
});

// ---------------------------------------------------------------------------
// isTitleMeaningful — tested indirectly:
//   meaningful title  → immediate sendNativeMessage
//   non-meaningful    → parked (no immediate send)
// ---------------------------------------------------------------------------
describe('isTitleMeaningful (indirect)', () => {
  const URL = 'https://example.com/';

  test('real title → flushes immediately', () => {
    tabReturns('Example Domain');
    navHandler({ frameId: 0, tabId: 1, url: URL });
    expect(mockSendNativeMessage).toHaveBeenCalledTimes(1);
    expect(mockSendNativeMessage.mock.calls[0][1].title).toBe('Example Domain');
  });

  test('empty title → parked (no immediate flush)', () => {
    tabReturns('');
    navHandler({ frameId: 0, tabId: 1, url: URL });
    expect(mockSendNativeMessage).not.toHaveBeenCalled();
  });

  test('title equals hostname → parked', () => {
    tabReturns('example.com');
    navHandler({ frameId: 0, tabId: 1, url: URL });
    expect(mockSendNativeMessage).not.toHaveBeenCalled();
  });

  test('title equals full URL → parked', () => {
    tabReturns(URL);
    navHandler({ frameId: 0, tabId: 1, url: URL });
    expect(mockSendNativeMessage).not.toHaveBeenCalled();
  });

  test('whitespace-only title → parked', () => {
    tabReturns('   ');
    navHandler({ frameId: 0, tabId: 1, url: URL });
    expect(mockSendNativeMessage).not.toHaveBeenCalled();
  });
});

// ---------------------------------------------------------------------------
// Navigation event — immediate flush path
// ---------------------------------------------------------------------------
describe('webNavigation.onCompleted — immediate flush', () => {
  test('ignores iframe navigations (frameId !== 0)', () => {
    navHandler({ frameId: 1, tabId: 1, url: 'https://example.com/' });
    expect(mockTabsGet).not.toHaveBeenCalled();
    expect(mockSendNativeMessage).not.toHaveBeenCalled();
  });

  test('does nothing when tabs.get sets lastError', () => {
    tabReturnsError('Tab was closed');
    navHandler({ frameId: 0, tabId: 1, url: 'https://example.com/' });
    expect(mockSendNativeMessage).not.toHaveBeenCalled();
  });

  test('message includes timestamp, url, and title', () => {
    tabReturns('Example Domain');
    navHandler({ frameId: 0, tabId: 1, url: 'https://example.com/' });
    const msg = mockSendNativeMessage.mock.calls[0][1];
    expect(msg).toHaveProperty('timestamp');
    expect(msg.url).toBe('https://example.com/');
    expect(msg.title).toBe('Example Domain');
  });

  test('timestamp is a valid ISO string that round-trips through Date', () => {
    tabReturns('Example Domain');
    navHandler({ frameId: 0, tabId: 1, url: 'https://example.com/' });
    const { timestamp } = mockSendNativeMessage.mock.calls[0][1];
    // new Date(x).toISOString() throws on Invalid Date, so this fails for garbage input
    const reparsed = new Date(timestamp);
    expect(isNaN(reparsed.getTime())).toBe(false);
    expect(reparsed.getFullYear()).toBeGreaterThan(2000);
  });

  test('sends to the correct native host name', () => {
    tabReturns('Example Domain');
    navHandler({ frameId: 0, tabId: 1, url: 'https://example.com/' });
    expect(mockSendNativeMessage.mock.calls[0][0]).toBe('com.browser.visit.logger');
  });
});

// ---------------------------------------------------------------------------
// Navigation event — deferred flush path (title not yet available)
// ---------------------------------------------------------------------------
describe('webNavigation.onCompleted — deferred flush', () => {
  test('flushes via timeout with URL as title fallback after 5 s', () => {
    tabReturns('');
    navHandler({ frameId: 0, tabId: 1, url: 'https://example.com/' });
    expect(mockSendNativeMessage).not.toHaveBeenCalled();

    jest.advanceTimersByTime(5000);

    expect(mockSendNativeMessage).toHaveBeenCalledTimes(1);
    const msg = mockSendNativeMessage.mock.calls[0][1];
    expect(msg.url).toBe('https://example.com/');
    expect(msg.title).toBe('https://example.com/');
  });

  test('does not flush before 5 s timeout', () => {
    tabReturns('');
    navHandler({ frameId: 0, tabId: 1, url: 'https://example.com/' });
    jest.advanceTimersByTime(4999);
    expect(mockSendNativeMessage).not.toHaveBeenCalled();
  });

  test('rapid navigation on same tab cancels first timer', () => {
    tabReturns('');
    navHandler({ frameId: 0, tabId: 1, url: 'https://first.com/' });
    navHandler({ frameId: 0, tabId: 1, url: 'https://second.com/' });

    jest.advanceTimersByTime(5000);

    // Only one flush — for the second URL
    expect(mockSendNativeMessage).toHaveBeenCalledTimes(1);
    expect(mockSendNativeMessage.mock.calls[0][1].url).toBe('https://second.com/');
  });

  test('different tabs are tracked independently', () => {
    tabReturns('');
    navHandler({ frameId: 0, tabId: 1, url: 'https://tab1.com/' });
    navHandler({ frameId: 0, tabId: 2, url: 'https://tab2.com/' });

    jest.advanceTimersByTime(5000);

    expect(mockSendNativeMessage).toHaveBeenCalledTimes(2);
    const urls = mockSendNativeMessage.mock.calls.map(c => c[1].url);
    expect(urls).toContain('https://tab1.com/');
    expect(urls).toContain('https://tab2.com/');
  });
});

// ---------------------------------------------------------------------------
// tabs.onUpdated — title arrives after navigation
// ---------------------------------------------------------------------------
describe('tabs.onUpdated', () => {
  test('flushes immediately when pending tab gets a title', () => {
    tabReturns('');
    navHandler({ frameId: 0, tabId: 1, url: 'https://example.com/' });
    expect(mockSendNativeMessage).not.toHaveBeenCalled();

    tabUpdateHandler(1, { title: 'Real Page Title' }, {});

    expect(mockSendNativeMessage).toHaveBeenCalledTimes(1);
    const msg = mockSendNativeMessage.mock.calls[0][1];
    expect(msg.title).toBe('Real Page Title');
    expect(msg.url).toBe('https://example.com/');
  });

  test('title update cancels the pending timeout', () => {
    tabReturns('');
    navHandler({ frameId: 0, tabId: 1, url: 'https://example.com/' });

    tabUpdateHandler(1, { title: 'Real Title' }, {});
    expect(mockSendNativeMessage).toHaveBeenCalledTimes(1);

    // Advancing past the original timeout must not produce a second send
    jest.advanceTimersByTime(5000);
    expect(mockSendNativeMessage).toHaveBeenCalledTimes(1);
  });

  test('ignores update for tab not in pending map', () => {
    tabUpdateHandler(99, { title: 'Some Title' }, {});
    expect(mockSendNativeMessage).not.toHaveBeenCalled();
  });

  test('ignores onUpdated with no title in changeInfo', () => {
    tabReturns('');
    navHandler({ frameId: 0, tabId: 1, url: 'https://example.com/' });

    tabUpdateHandler(1, { status: 'complete' }, {}); // no title key

    expect(mockSendNativeMessage).not.toHaveBeenCalled();
  });

  test('flushed message contains original URL and a valid navigation timestamp', () => {
    tabReturns('');
    navHandler({ frameId: 0, tabId: 1, url: 'https://example.com/' });

    tabUpdateHandler(1, { title: 'Late Title' }, {});

    const msg = mockSendNativeMessage.mock.calls[0][1];
    expect(msg.url).toBe('https://example.com/');
    expect(msg.title).toBe('Late Title');
    // Timestamp must be a valid date, not just any truthy value
    const parsed = new Date(msg.timestamp);
    expect(isNaN(parsed.getTime())).toBe(false);
    expect(parsed.getFullYear()).toBeGreaterThan(2000);
  });
});
