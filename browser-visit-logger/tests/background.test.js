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
const addMessageListener    = jest.fn();

// Snapshot-related mocks
const mockSaveAsMHTML       = jest.fn();
const mockDownloadsDownload = jest.fn();
const mockDownloadsSearch   = jest.fn();

// FileReader mock — readAsDataURL fires 'loadend' synchronously by default.
// Tests that need to simulate a read error can set mockFileReaderInstance.error.
let mockFileReaderInstance;
function buildFileReaderMock() {
  mockFileReaderInstance = {
    addEventListener: jest.fn(),
    readAsDataURL:    jest.fn(),
    result: 'data:message/rfc822;base64,MOCKED_DATA',
    error:  null,
  };
  mockFileReaderInstance.readAsDataURL.mockImplementation(() => {
    for (const [event, cb] of mockFileReaderInstance.addEventListener.mock.calls) {
      if (event === 'loadend') cb();
    }
  });
  global.FileReader = jest.fn(() => mockFileReaderInstance);
}

// downloads.onChanged needs add/removeListener so tests can fire deltas
const onChangedListeners = [];
const mockDownloadsOnChanged = {
  addListener:    jest.fn((cb) => onChangedListeners.push(cb)),
  removeListener: jest.fn((cb) => {
    const i = onChangedListeners.indexOf(cb);
    if (i !== -1) onChangedListeners.splice(i, 1);
  }),
};

function fireDownloadChanged(delta) {
  [...onChangedListeners].forEach((cb) => cb(delta));
}

function buildChromeMock() {

  global.chrome = {
    runtime: {
      sendNativeMessage: mockSendNativeMessage,
      lastError: null,
      onMessage: { addListener: addMessageListener },
    },
    webNavigation: {
      onCompleted: { addListener: addNavListener },
    },
    tabs: {
      get:       mockTabsGet,
      onUpdated: { addListener: addTabUpdateListener },
    },
    pageCapture: {
      saveAsMHTML: mockSaveAsMHTML,
    },
    downloads: {
      download:  mockDownloadsDownload,
      search:    mockDownloadsSearch,
      onChanged: mockDownloadsOnChanged,
    },
  };
}

// ---------------------------------------------------------------------------
// Load a fresh copy of background.js before every test
// (jest.resetModules() clears the module cache so pendingVisits starts empty)
// ---------------------------------------------------------------------------
let navHandler, tabUpdateHandler, messageHandler;

beforeEach(() => {
  jest.useFakeTimers();

  // Clear mock state from previous test
  mockSendNativeMessage.mockClear();
  mockTabsGet.mockClear();
  addNavListener.mockClear();
  addTabUpdateListener.mockClear();
  addMessageListener.mockClear();
  mockSaveAsMHTML.mockClear();
  mockDownloadsDownload.mockClear();
  mockDownloadsSearch.mockClear();
  mockDownloadsOnChanged.addListener.mockClear();
  mockDownloadsOnChanged.removeListener.mockClear();
  onChangedListeners.length = 0;

  // Fresh chrome mock and FileReader mock
  buildChromeMock();
  buildFileReaderMock();

  // Fresh module (new pendingVisits Map, fresh listener registrations)
  jest.resetModules();
  require('../extension/background');

  navHandler       = addNavListener.mock.calls[0][0];
  tabUpdateHandler = addTabUpdateListener.mock.calls[0][0];
  messageHandler   = addMessageListener.mock.calls[0][0];
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

  test('registers a runtime.onMessage listener', () => {
    expect(addMessageListener).toHaveBeenCalledTimes(1);
    expect(typeof messageHandler).toBe('function');
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

  test('ignores onUpdated with whitespace-only title; timeout falls back to URL', () => {
    tabReturns('');
    navHandler({ frameId: 0, tabId: 1, url: 'https://example.com/' });

    // Whitespace is truthy in JS but not a meaningful title — must not flush early
    tabUpdateHandler(1, { title: '   ' }, {});
    expect(mockSendNativeMessage).not.toHaveBeenCalled();

    // Timeout fires — URL used as fallback since no meaningful title ever arrived
    jest.advanceTimersByTime(5000);
    expect(mockSendNativeMessage).toHaveBeenCalledTimes(1);
    expect(mockSendNativeMessage.mock.calls[0][1].title).toBe('https://example.com/');
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

// ---------------------------------------------------------------------------
// read-and-snapshot message handler
// ---------------------------------------------------------------------------
describe('read-and-snapshot message handler', () => {
  const baseMsg = {
    type:      'read-and-snapshot',
    tabId:     1,
    timestamp: '2026-01-01T00:00:00Z',
    url:       'https://example.com/',
    title:     'Example',
  };

  // Helper: set up mocks for a fully successful snapshot flow
  function setupSuccessFlow({ downloadId = 42, filename = '/tmp/bvl-snapshot-123.mhtml' } = {}) {
    const fakeBlob = { type: 'message/rfc822' }; // stand-in for a Blob
    mockSaveAsMHTML.mockImplementation(({ tabId }, cb) => cb(fakeBlob));
    mockDownloadsDownload.mockImplementation((opts, cb) => cb(downloadId));
    mockDownloadsSearch.mockImplementation(({ id }, cb) => cb([{ filename }]));
    mockSendNativeMessage.mockImplementation((host, msg, cb) => cb({ status: 'ok' }));
  }

  test('ignores messages of unknown type', () => {
    const result = messageHandler({ type: 'something-else' }, {}, jest.fn());
    expect(result).toBe(false);
    expect(mockSaveAsMHTML).not.toHaveBeenCalled();
  });

  test('calls pageCapture.saveAsMHTML with the tabId', () => {
    setupSuccessFlow();
    messageHandler(baseMsg, {}, jest.fn());
    expect(mockSaveAsMHTML).toHaveBeenCalledWith({ tabId: 1 }, expect.any(Function));
  });

  test('on pageCapture error, calls sendResponse with error', () => {
    mockSaveAsMHTML.mockImplementation(({ tabId }, cb) => {
      global.chrome.runtime.lastError = { message: 'Tab not found' };
      cb(null);
      global.chrome.runtime.lastError = null;
    });
    const sendResponse = jest.fn();
    messageHandler(baseMsg, {}, sendResponse);
    expect(sendResponse).toHaveBeenCalledWith(expect.objectContaining({ status: 'error' }));
    expect(mockDownloadsDownload).not.toHaveBeenCalled();
  });

  test('on successful capture, calls downloads.download with an .mhtml filename', () => {
    setupSuccessFlow();
    messageHandler(baseMsg, {}, jest.fn());
    expect(mockDownloadsDownload).toHaveBeenCalledWith(
      expect.objectContaining({ filename: expect.stringMatching(/\.mhtml$/), saveAs: false }),
      expect.any(Function),
    );
  });

  test('reads blob as data URL and passes it to downloads.download', () => {
    setupSuccessFlow();
    messageHandler(baseMsg, {}, jest.fn());
    expect(mockFileReaderInstance.readAsDataURL).toHaveBeenCalledTimes(1);
    expect(mockDownloadsDownload).toHaveBeenCalledWith(
      expect.objectContaining({ url: 'data:message/rfc822;base64,MOCKED_DATA' }),
      expect.any(Function),
    );
  });

  test('on download error, calls sendResponse with error', () => {
    const fakeBlob = { type: 'message/rfc822' };
    mockSaveAsMHTML.mockImplementation(({ tabId }, cb) => cb(fakeBlob));
    mockDownloadsDownload.mockImplementation((opts, cb) => {
      global.chrome.runtime.lastError = { message: 'Download failed' };
      cb(undefined);
      global.chrome.runtime.lastError = null;
    });
    const sendResponse = jest.fn();
    messageHandler(baseMsg, {}, sendResponse);
    expect(sendResponse).toHaveBeenCalledWith(expect.objectContaining({ status: 'error' }));
    expect(mockDownloadsOnChanged.addListener).not.toHaveBeenCalled();
  });

  test('listens for download completion after successful download start', () => {
    setupSuccessFlow();
    messageHandler(baseMsg, {}, jest.fn());
    expect(mockDownloadsOnChanged.addListener).toHaveBeenCalledTimes(1);
  });

  test('on download complete, searches for the file path and sends native message', () => {
    setupSuccessFlow({ downloadId: 42, filename: '/tmp/bvl-snapshot-123.mhtml' });
    messageHandler(baseMsg, {}, jest.fn());

    fireDownloadChanged({ id: 42, state: { current: 'complete' } });

    expect(mockDownloadsSearch).toHaveBeenCalledWith({ id: 42 }, expect.any(Function));
    expect(mockSendNativeMessage).toHaveBeenCalledWith(
      'com.browser.visit.logger',
      expect.objectContaining({
        tag:                    'read',
        url:                    'https://example.com/',
        snapshot_download_path: '/tmp/bvl-snapshot-123.mhtml',
      }),
      expect.any(Function),
    );
  });

  test('ignores download change events for other download IDs', () => {
    setupSuccessFlow({ downloadId: 42 });
    messageHandler(baseMsg, {}, jest.fn());

    fireDownloadChanged({ id: 99, state: { current: 'complete' } });

    expect(mockDownloadsSearch).not.toHaveBeenCalled();
  });

  test('on download interrupted, calls sendResponse with error', () => {
    setupSuccessFlow({ downloadId: 42 });
    const sendResponse = jest.fn();
    messageHandler(baseMsg, {}, sendResponse);

    fireDownloadChanged({ id: 42, state: { current: 'interrupted' } });

    expect(sendResponse).toHaveBeenCalledWith(expect.objectContaining({ status: 'error' }));
    expect(mockDownloadsOnChanged.removeListener).toHaveBeenCalled();
  });

  test('on native message success, calls sendResponse with ok', () => {
    setupSuccessFlow();
    const sendResponse = jest.fn();
    messageHandler(baseMsg, {}, sendResponse);

    fireDownloadChanged({ id: 42, state: { current: 'complete' } });

    expect(sendResponse).toHaveBeenCalledWith({ status: 'ok' });
  });

  test('on native message error, calls sendResponse with error', () => {
    const fakeBlob = { type: 'message/rfc822' };
    mockSaveAsMHTML.mockImplementation(({ tabId }, cb) => cb(fakeBlob));
    mockDownloadsDownload.mockImplementation((opts, cb) => cb(42));
    mockDownloadsSearch.mockImplementation(({ id }, cb) => cb([{ filename: '/tmp/snap.mhtml' }]));
    mockSendNativeMessage.mockImplementation((host, msg, cb) => {
      global.chrome.runtime.lastError = { message: 'Native host error' };
      cb(null);
      global.chrome.runtime.lastError = null;
    });
    const sendResponse = jest.fn();
    messageHandler(baseMsg, {}, sendResponse);
    fireDownloadChanged({ id: 42, state: { current: 'complete' } });
    expect(sendResponse).toHaveBeenCalledWith(expect.objectContaining({ status: 'error' }));
  });

  test('removes onChanged listener after completion', () => {
    setupSuccessFlow({ downloadId: 42 });
    messageHandler(baseMsg, {}, jest.fn());
    fireDownloadChanged({ id: 42, state: { current: 'complete' } });
    expect(mockDownloadsOnChanged.removeListener).toHaveBeenCalled();
  });

  test('returns true to keep message channel open', () => {
    setupSuccessFlow();
    const result = messageHandler(baseMsg, {}, jest.fn());
    expect(result).toBe(true);
  });
});
