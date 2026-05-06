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
// Visit-log / tag-write native messages.
const mockSendNativeMessage = jest.fn();
// Icon-refresh query messages ({ action: 'query', url }).  Routed
// separately so existing visit-log assertions aren't perturbed by
// the icon refresh that fires on every navigation.
const mockQueryNativeMessage = jest.fn();
const mockTabsGet           = jest.fn();
const addNavListener        = jest.fn();
const addTabUpdateListener  = jest.fn();
const addTabActivatedListener = jest.fn();
const addMessageListener    = jest.fn();
const mockSetIcon           = jest.fn((_arg, cb) => { if (cb) cb(); });

// Snapshot-related mocks
const mockSaveAsMHTML       = jest.fn();
const mockDownloadsDownload = jest.fn();

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

// Fixed 32-byte SHA-256 hash buffer returned by the crypto.subtle mock.
// Hex: 'abababab...' (64 chars)
const MOCK_HASH_BUFFER = new Uint8Array(32).fill(0xab).buffer;
const MOCK_HEX_HASH   = 'ab'.repeat(32);

function buildChromeMock() {
  global.crypto = {
    subtle: {
      digest: jest.fn(() => Promise.resolve(MOCK_HASH_BUFFER)),
    },
  };

  // Minimal OffscreenCanvas stub — background.js draws a colored circle into
  // it and reads back ImageData; tests only care that getImageData returns a
  // truthy value tagged with the chosen fill color.
  global.OffscreenCanvas = jest.fn((width, height) => {
    // Track the disk fill (set before the arc) and any subsequent
    // text fill (set before fillText) so tests can assert both.
    let diskFill = null;
    let lastFill = null;
    let drewText = null;
    return {
      width, height,
      getContext: () => ({
        set fillStyle(c) { lastFill = c; },
        get fillStyle()   { return lastFill; },
        font: '',
        textAlign: '',
        textBaseline: '',
        beginPath: () => {},
        arc:       () => { diskFill = lastFill; },
        fill:      () => {},
        fillText:  (text) => { drewText = { text, fillStyle: lastFill }; },
        getImageData: (_x, _y, w, h) => ({
          width: w, height: h, color: diskFill, glyph: drewText,
        }),
      }),
    };
  });

  global.chrome = {
    runtime: {
      sendNativeMessage: (host, msg, cb) => (
        msg && msg.action === 'query'
          ? mockQueryNativeMessage(host, msg, cb)
          : mockSendNativeMessage(host, msg, cb)
      ),
      sendMessage:       jest.fn(),
      lastError: null,
      onMessage: { addListener: addMessageListener },
    },
    webNavigation: {
      onCompleted: { addListener: addNavListener },
    },
    tabs: {
      get:         mockTabsGet,
      onUpdated:   { addListener: addTabUpdateListener },
      onActivated: { addListener: addTabActivatedListener },
    },
    action: {
      setIcon: mockSetIcon,
    },
    pageCapture: {
      saveAsMHTML: mockSaveAsMHTML,
    },
    downloads: {
      download:  mockDownloadsDownload,
      onChanged: mockDownloadsOnChanged,
    },
  };
}

// ---------------------------------------------------------------------------
// Load a fresh copy of background.js before every test
// (jest.resetModules() clears the module cache so pendingVisits starts empty)
// ---------------------------------------------------------------------------
let navHandler, tabUpdateHandler, tabActivatedHandler, messageHandler;

beforeEach(() => {
  jest.useFakeTimers();

  // Clear mock state from previous test
  mockSendNativeMessage.mockClear();
  mockQueryNativeMessage.mockClear();
  mockTabsGet.mockClear();
  addNavListener.mockClear();
  addTabUpdateListener.mockClear();
  addTabActivatedListener.mockClear();
  addMessageListener.mockClear();
  mockSaveAsMHTML.mockClear();
  mockDownloadsDownload.mockClear();
  mockSetIcon.mockClear();
  mockDownloadsOnChanged.addListener.mockClear();
  mockDownloadsOnChanged.removeListener.mockClear();
  onChangedListeners.length = 0;

  // Fresh chrome mock and FileReader mock
  buildChromeMock();
  buildFileReaderMock();

  // Fresh module (new pendingVisits Map, fresh listener registrations)
  jest.resetModules();
  require('../extension/background');

  navHandler          = addNavListener.mock.calls[0][0];
  tabUpdateHandler    = addTabUpdateListener.mock.calls[0][0];
  tabActivatedHandler = addTabActivatedListener.mock.calls[0][0];
  messageHandler      = addMessageListener.mock.calls[0][0];
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
// Error-handling branches in sendNativeMessage callbacks
// ---------------------------------------------------------------------------
describe('sendNativeMessage error handling', () => {
  test('immediate-flush callback logs error when lastError is set', () => {
    const consoleSpy = jest.spyOn(console, 'error').mockImplementation(() => {});
    mockSendNativeMessage.mockImplementation((_host, _msg, cb) => {
      global.chrome.runtime.lastError = { message: 'Host not found' };
      cb(null);
      global.chrome.runtime.lastError = null;
    });
    tabReturns('Real Title');
    navHandler({ frameId: 0, tabId: 1, url: 'https://example.com/' });
    expect(consoleSpy).toHaveBeenCalledWith(
      expect.stringContaining('[BVL]'), expect.any(String),
    );
    consoleSpy.mockRestore();
  });

  test('flushVisit callback logs error when lastError is set', () => {
    const consoleSpy = jest.spyOn(console, 'error').mockImplementation(() => {});
    mockSendNativeMessage.mockImplementation((_host, _msg, cb) => {
      global.chrome.runtime.lastError = { message: 'Host not found' };
      cb(null);
      global.chrome.runtime.lastError = null;
    });
    tabReturns('');
    navHandler({ frameId: 0, tabId: 1, url: 'https://example.com/' });
    // Title arrives, triggers flushVisit → sendNativeMessage
    tabUpdateHandler(1, { title: 'Real Title' }, {});
    expect(consoleSpy).toHaveBeenCalledWith(
      expect.stringContaining('[BVL]'), expect.any(String),
    );
    consoleSpy.mockRestore();
  });
});

// ---------------------------------------------------------------------------
// isTitleMeaningful — catch branch (invalid URL)
// ---------------------------------------------------------------------------
describe('isTitleMeaningful — invalid URL catch branch', () => {
  test('immediate-flush callback with no lastError does not log an error', () => {
    const consoleSpy = jest.spyOn(console, 'error').mockImplementation(() => {});
    // Invoke the callback successfully (no lastError) so the false-branch of the if is taken
    mockSendNativeMessage.mockImplementation((_host, _msg, cb) => cb({ status: 'ok' }));
    tabReturns('Real Title');
    navHandler({ frameId: 0, tabId: 1, url: 'https://example.com/' });
    expect(consoleSpy).not.toHaveBeenCalled();
    consoleSpy.mockRestore();
  });

  test('flushVisit callback with no lastError does not log an error', () => {
    const consoleSpy = jest.spyOn(console, 'error').mockImplementation(() => {});
    mockSendNativeMessage.mockImplementation((_host, _msg, cb) => cb({ status: 'ok' }));
    tabReturns('');
    navHandler({ frameId: 0, tabId: 1, url: 'https://example.com/' });
    tabUpdateHandler(1, { title: 'Real Title' }, {});
    expect(consoleSpy).not.toHaveBeenCalled();
    consoleSpy.mockRestore();
  });

  test('non-parseable URL is handled gracefully; meaningful title still flushes immediately', () => {
    tabReturns('Some Title');
    // 'not-a-valid-url' throws inside new URL() — the catch block swallows it
    // and isTitleMeaningful returns true (title is non-empty and not the URL itself)
    navHandler({ frameId: 0, tabId: 1, url: 'not-a-valid-url' });
    expect(mockSendNativeMessage).toHaveBeenCalledTimes(1);
    expect(mockSendNativeMessage.mock.calls[0][1].title).toBe('Some Title');
  });
});

// ---------------------------------------------------------------------------
// Defensive null-tab branch (tabs.get returns null without lastError)
// ---------------------------------------------------------------------------
describe('tabs.get null-tab defensive branch', () => {
  test('null tab without lastError is treated as empty title and parked', () => {
    mockTabsGet.mockImplementation((_tabId, cb) => cb(null)); // null tab, no lastError
    navHandler({ frameId: 0, tabId: 1, url: 'https://example.com/' });
    expect(mockSendNativeMessage).not.toHaveBeenCalled();
    // Falls back to URL title after timeout
    jest.advanceTimersByTime(5000);
    expect(mockSendNativeMessage).toHaveBeenCalledTimes(1);
    expect(mockSendNativeMessage.mock.calls[0][1].title).toBe('https://example.com/');
  });
});

// ---------------------------------------------------------------------------
// tag-and-snapshot message handler
// ---------------------------------------------------------------------------
describe('tag-and-snapshot message handler', () => {
  const baseMsg = {
    type:      'tag-and-snapshot',
    tag:       'read',
    tabId:     1,
    timestamp: '2026-01-01T00:00:00Z',
    url:       'https://example.com/',
    title:     'Example',
  };

  // Helper to flush Promise microtasks so crypto.subtle.digest and Promise.all resolve.
  async function flushPromises() {
    await Promise.resolve();
    await Promise.resolve();
    await Promise.resolve();
  }

  // Helper: set up mocks for a fully successful snapshot flow
  function setupSuccessFlow({ downloadId = 42 } = {}) {
    const fakeBlob = { type: 'message/rfc822' }; // stand-in for a Blob
    mockSaveAsMHTML.mockImplementation(({ tabId }, cb) => cb(fakeBlob));
    mockDownloadsDownload.mockImplementation((opts, cb) => cb(downloadId));
    mockSendNativeMessage.mockImplementation((host, msg, cb) => cb({ status: 'ok' }));
  }

  test('ignores messages of unknown type', () => {
    const result = messageHandler({ type: 'something-else' }, {}, jest.fn());
    expect(result).toBe(false);
    expect(mockSaveAsMHTML).not.toHaveBeenCalled();
  });

  // --- HTML page path (saveAsMHTML) ---

  test('calls pageCapture.saveAsMHTML with the tabId for non-PDF pages', () => {
    setupSuccessFlow();
    messageHandler(baseMsg, {}, jest.fn());
    expect(mockSaveAsMHTML).toHaveBeenCalledWith({ tabId: 1 }, expect.any(Function));
  });

  test('on pageCapture error, calls sendResponse with error', async () => {
    mockSaveAsMHTML.mockImplementation(({ tabId }, cb) => {
      global.chrome.runtime.lastError = { message: 'Tab not found' };
      cb(null);
      global.chrome.runtime.lastError = null;
    });
    const sendResponse = jest.fn();
    messageHandler(baseMsg, {}, sendResponse);
    await flushPromises();
    expect(sendResponse).toHaveBeenCalledWith(expect.objectContaining({ status: 'error' }));
    expect(mockDownloadsDownload).not.toHaveBeenCalled();
  });

  test('HTML page: downloads to browser-visit-snapshots/<sha256>.mhtml', async () => {
    setupSuccessFlow();
    messageHandler(baseMsg, {}, jest.fn());
    await flushPromises();
    expect(mockDownloadsDownload).toHaveBeenCalledWith(
      expect.objectContaining({
        filename: `browser-visit-snapshots/2026-01-01T00-00-00Z-${MOCK_HEX_HASH}.mhtml`,
        saveAs: false,
      }),
      expect.any(Function),
    );
  });

  test('HTML page: reads blob as data URL and passes it to downloads.download', async () => {
    setupSuccessFlow();
    messageHandler(baseMsg, {}, jest.fn());
    await flushPromises();
    expect(mockFileReaderInstance.readAsDataURL).toHaveBeenCalledTimes(1);
    expect(mockDownloadsDownload).toHaveBeenCalledWith(
      expect.objectContaining({ url: 'data:message/rfc822;base64,MOCKED_DATA' }),
      expect.any(Function),
    );
  });

  // --- PDF path (direct URL download) ---

  describe('PDF URL handling', () => {
    const pdfUrls = [
      'https://example.com/paper.pdf',
      'https://example.com/paper.PDF',
      'https://example.com/paper.pdf?v=2',
      'https://example.com/paper.pdf#page=3',
    ];
    const nonPdfUrls = [
      'https://example.com/',
      'https://example.com/page.html',
      'https://example.com/pdf-viewer',
    ];

    pdfUrls.forEach((url) => {
      test(`treats "${url}" as PDF`, async () => {
        mockDownloadsDownload.mockImplementation((opts, cb) => cb(42));
        mockSendNativeMessage.mockImplementation((host, msg, cb) => cb({ status: 'ok' }));
        const pdfMsg = { ...baseMsg, url };
        messageHandler(pdfMsg, {}, jest.fn());
        await flushPromises();
        // Should NOT call saveAsMHTML
        expect(mockSaveAsMHTML).not.toHaveBeenCalled();
        // Should download the original URL directly with .pdf extension under browser-visit-snapshots/
        expect(mockDownloadsDownload).toHaveBeenCalledWith(
          expect.objectContaining({ url, filename: expect.stringMatching(/^browser-visit-snapshots\/.*\.pdf$/) }),
          expect.any(Function),
        );
      });
    });

    nonPdfUrls.forEach((url) => {
      test(`treats "${url}" as HTML (uses saveAsMHTML)`, () => {
        setupSuccessFlow();
        messageHandler({ ...baseMsg, url }, {}, jest.fn());
        expect(mockSaveAsMHTML).toHaveBeenCalled();
      });
    });

    test('PDF: filename is browser-visit-snapshots/<sha256>.pdf', async () => {
      mockDownloadsDownload.mockImplementation((opts, cb) => cb(42));
      mockSendNativeMessage.mockImplementation((host, msg, cb) => cb({ status: 'ok' }));
      const pdfMsg = { ...baseMsg, url: 'https://example.com/paper.pdf' };
      messageHandler(pdfMsg, {}, jest.fn());
      await flushPromises();
      expect(mockDownloadsDownload).toHaveBeenCalledWith(
        expect.objectContaining({
          filename: `browser-visit-snapshots/2026-01-01T00-00-00Z-${MOCK_HEX_HASH}.pdf`,
          saveAs: false,
        }),
        expect.any(Function),
      );
    });

    test('PDF: does not call FileReader', async () => {
      mockDownloadsDownload.mockImplementation((opts, cb) => cb(42));
      mockSendNativeMessage.mockImplementation((host, msg, cb) => cb({ status: 'ok' }));
      messageHandler({ ...baseMsg, url: 'https://example.com/doc.pdf' }, {}, jest.fn());
      await flushPromises();
      expect(mockFileReaderInstance.readAsDataURL).not.toHaveBeenCalled();
    });
  });

  test('on download error, calls sendResponse with error', async () => {
    const fakeBlob = { type: 'message/rfc822' };
    mockSaveAsMHTML.mockImplementation(({ tabId }, cb) => cb(fakeBlob));
    mockDownloadsDownload.mockImplementation((opts, cb) => {
      global.chrome.runtime.lastError = { message: 'Download failed' };
      cb(undefined);
      global.chrome.runtime.lastError = null;
    });
    const sendResponse = jest.fn();
    messageHandler(baseMsg, {}, sendResponse);
    await flushPromises();
    expect(sendResponse).toHaveBeenCalledWith(expect.objectContaining({ status: 'error' }));
    expect(mockDownloadsOnChanged.addListener).not.toHaveBeenCalled();
  });

  test('listens for download completion after successful download start', async () => {
    setupSuccessFlow();
    messageHandler(baseMsg, {}, jest.fn());
    await flushPromises();
    expect(mockDownloadsOnChanged.addListener).toHaveBeenCalledTimes(1);
  });

  test('on download complete, sends native message with the tag and filename', async () => {
    setupSuccessFlow({ downloadId: 42 });
    messageHandler(baseMsg, {}, jest.fn());
    await flushPromises();

    fireDownloadChanged({ id: 42, state: { current: 'complete' } });

    expect(mockSendNativeMessage).toHaveBeenCalledWith(
      'com.browser.visit.logger',
      {
        tag: 'read', url: 'https://example.com/', timestamp: baseMsg.timestamp, title: baseMsg.title,
        filename: `browser-visit-snapshots/2026-01-01T00-00-00Z-${MOCK_HEX_HASH}.mhtml`,
      },
      expect.any(Function),
    );
  });

  test('skimmed tag: snapshot flow works and native message carries tag "skimmed" and filename', async () => {
    setupSuccessFlow({ downloadId: 43 });
    const skimMsg = { ...baseMsg, tag: 'skimmed' };
    const sendResponse = jest.fn();
    messageHandler(skimMsg, {}, sendResponse);
    await flushPromises();

    fireDownloadChanged({ id: 43, state: { current: 'complete' } });

    expect(mockSendNativeMessage).toHaveBeenCalledWith(
      'com.browser.visit.logger',
      {
        tag: 'skimmed', url: skimMsg.url, timestamp: skimMsg.timestamp, title: skimMsg.title,
        filename: `browser-visit-snapshots/2026-01-01T00-00-00Z-${MOCK_HEX_HASH}.mhtml`,
      },
      expect.any(Function),
    );
    expect(sendResponse).toHaveBeenCalledWith({ status: 'ok' });
  });

  test('ignores download change events for other download IDs', async () => {
    setupSuccessFlow({ downloadId: 42 });
    messageHandler(baseMsg, {}, jest.fn());
    await flushPromises();

    fireDownloadChanged({ id: 99, state: { current: 'complete' } });

    expect(mockSendNativeMessage).not.toHaveBeenCalled();
  });

  test('on download interrupted, calls sendResponse with error', async () => {
    setupSuccessFlow({ downloadId: 42 });
    const sendResponse = jest.fn();
    messageHandler(baseMsg, {}, sendResponse);
    await flushPromises();

    fireDownloadChanged({ id: 42, state: { current: 'interrupted' } });

    expect(sendResponse).toHaveBeenCalledWith(expect.objectContaining({ status: 'error' }));
    expect(mockDownloadsOnChanged.removeListener).toHaveBeenCalled();
  });

  test('on native message success, calls sendResponse with ok', async () => {
    setupSuccessFlow();
    const sendResponse = jest.fn();
    messageHandler(baseMsg, {}, sendResponse);
    await flushPromises();

    fireDownloadChanged({ id: 42, state: { current: 'complete' } });

    expect(sendResponse).toHaveBeenCalledWith({ status: 'ok' });
  });

  test('on native message error response (no lastError), forwards error to sendResponse', async () => {
    // Covers the else-branch: host returns {status:'error'} without a Chrome lastError
    const fakeBlob = { type: 'message/rfc822' };
    mockSaveAsMHTML.mockImplementation(({ tabId }, cb) => cb(fakeBlob));
    mockDownloadsDownload.mockImplementation((opts, cb) => cb(42));
    mockSendNativeMessage.mockImplementation((host, msg, cb) =>
      cb({ status: 'error', message: 'Write failed' })
    );
    const sendResponse = jest.fn();
    messageHandler(baseMsg, {}, sendResponse);
    await flushPromises();
    fireDownloadChanged({ id: 42, state: { current: 'complete' } });
    expect(sendResponse).toHaveBeenCalledWith(
      expect.objectContaining({ status: 'error', message: 'Write failed' })
    );
  });

  test('on native message error (lastError), calls sendResponse with error', async () => {
    const fakeBlob = { type: 'message/rfc822' };
    mockSaveAsMHTML.mockImplementation(({ tabId }, cb) => cb(fakeBlob));
    mockDownloadsDownload.mockImplementation((opts, cb) => cb(42));
    mockSendNativeMessage.mockImplementation((host, msg, cb) => {
      global.chrome.runtime.lastError = { message: 'Native host error' };
      cb(null);
      global.chrome.runtime.lastError = null;
    });
    const sendResponse = jest.fn();
    messageHandler(baseMsg, {}, sendResponse);
    await flushPromises();
    fireDownloadChanged({ id: 42, state: { current: 'complete' } });
    expect(sendResponse).toHaveBeenCalledWith(expect.objectContaining({ status: 'error' }));
  });

  test('removes onChanged listener after completion', async () => {
    setupSuccessFlow({ downloadId: 42 });
    messageHandler(baseMsg, {}, jest.fn());
    await flushPromises();
    fireDownloadChanged({ id: 42, state: { current: 'complete' } });
    expect(mockDownloadsOnChanged.removeListener).toHaveBeenCalled();
  });

  test('returns true to keep message channel open', () => {
    setupSuccessFlow();
    const result = messageHandler(baseMsg, {}, jest.fn());
    expect(result).toBe(true);
  });

  test('pageCapture null data without lastError uses "no data" fallback in error message', async () => {
    // The condition is `lastError || !mhtmlData`; here lastError is null but data is null,
    // so the `|| 'no data'` right-hand side of the error string is taken.
    mockSaveAsMHTML.mockImplementation(({ tabId }, cb) => cb(null)); // no lastError, null data
    const sendResponse = jest.fn();
    messageHandler(baseMsg, {}, sendResponse);
    await flushPromises();
    expect(sendResponse).toHaveBeenCalledWith(
      expect.objectContaining({ status: 'error', message: expect.stringContaining('no data') }),
    );
  });

  test('FileReader error rejects contentPromise and calls sendResponse with error', async () => {
    // Triggers the `if (reader.error) reject(reader.error)` true-branch.
    const fakeBlob = { type: 'message/rfc822' };
    mockSaveAsMHTML.mockImplementation(({ tabId }, cb) => cb(fakeBlob));
    mockFileReaderInstance.error = new Error('Read failed');
    const sendResponse = jest.fn();
    messageHandler(baseMsg, {}, sendResponse);
    await flushPromises();
    expect(sendResponse).toHaveBeenCalledWith(expect.objectContaining({ status: 'error' }));
    expect(mockDownloadsDownload).not.toHaveBeenCalled();
  });

  test('download failure with lastError but no message field uses "unknown" fallback', async () => {
    // Triggers the `|| 'unknown'` branch in the downloads.download callback.
    const fakeBlob = { type: 'message/rfc822' };
    mockSaveAsMHTML.mockImplementation(({ tabId }, cb) => cb(fakeBlob));
    mockDownloadsDownload.mockImplementation((opts, cb) => {
      global.chrome.runtime.lastError = {}; // set but no .message property
      cb(undefined);
      global.chrome.runtime.lastError = null;
    });
    const sendResponse = jest.fn();
    messageHandler(baseMsg, {}, sendResponse);
    await flushPromises();
    expect(sendResponse).toHaveBeenCalledWith(
      expect.objectContaining({ status: 'error', message: expect.stringContaining('unknown') }),
    );
  });

  test('download state changes other than complete/interrupted are ignored', async () => {
    // Triggers the false-branch of `else if (delta.state?.current === 'interrupted')`.
    setupSuccessFlow({ downloadId: 42 });
    const sendResponse = jest.fn();
    messageHandler(baseMsg, {}, sendResponse);
    await flushPromises();

    fireDownloadChanged({ id: 42, state: { current: 'in_progress' } });

    // Neither complete nor interrupted — sendResponse should not have been called yet
    expect(sendResponse).not.toHaveBeenCalled();
    // The listener should still be registered (not removed)
    expect(mockDownloadsOnChanged.removeListener).not.toHaveBeenCalled();
  });
});

// ---------------------------------------------------------------------------
// Address-bar icon coloring
//
// The OffscreenCanvas mock tags getImageData() output with the fillStyle so
// these tests can assert "what color did setIcon receive?" without needing a
// real canvas implementation.
// ---------------------------------------------------------------------------
describe('address-bar icon coloring', () => {
  const URL = 'https://example.com/';

  // Color literals must match background.js.
  const GRAY   = '#9e9e9e';
  const ORANGE = '#ff9800';
  const YELLOW = '#ffeb3b';
  const GREEN  = '#4caf50';

  function lastIconColor() {
    const calls = mockSetIcon.mock.calls;
    if (calls.length === 0) return null;
    const { imageData } = calls[calls.length - 1][0];
    // Both sizes always carry the same fill — read either.
    return imageData[16].color;
  }

  function lastIconTabId() {
    const calls = mockSetIcon.mock.calls;
    return calls[calls.length - 1][0].tabId;
  }

  function respondWith(record) {
    mockQueryNativeMessage.mockImplementation((_host, _msg, cb) =>
      cb({ status: 'ok', record }),
    );
  }

  describe('listener registration', () => {
    test('registers a tabs.onActivated listener', () => {
      expect(addTabActivatedListener).toHaveBeenCalledTimes(1);
      expect(typeof tabActivatedHandler).toBe('function');
    });
  });

  describe('pickIconColor priority (via tabs.onActivated)', () => {
    beforeEach(() => {
      mockTabsGet.mockImplementation((_tabId, cb) => cb({ url: URL }));
    });

    test('null record → gray', () => {
      respondWith(null);
      tabActivatedHandler({ tabId: 7 });
      expect(lastIconColor()).toBe(GRAY);
      expect(lastIconTabId()).toBe(7);
    });

    test('empty record → gray', () => {
      respondWith({ read: [], skimmed: [], of_interest: null });
      tabActivatedHandler({ tabId: 7 });
      expect(lastIconColor()).toBe(GRAY);
    });

    test('of_interest only → orange', () => {
      respondWith({ read: [], skimmed: [], of_interest: '1' });
      tabActivatedHandler({ tabId: 7 });
      expect(lastIconColor()).toBe(ORANGE);
    });

    test('skimmed (no read) → yellow', () => {
      respondWith({ read: [], skimmed: [{ timestamp: 't' }], of_interest: '1' });
      tabActivatedHandler({ tabId: 7 });
      expect(lastIconColor()).toBe(YELLOW);
    });

    test('read present → green (overrides skimmed and of_interest)', () => {
      respondWith({
        read:        [{ timestamp: 't' }],
        skimmed:     [{ timestamp: 't' }],
        of_interest: '1',
      });
      tabActivatedHandler({ tabId: 7 });
      expect(lastIconColor()).toBe(GREEN);
    });

    test('record missing read/skimmed arrays falls through to gray', () => {
      respondWith({ of_interest: null });
      tabActivatedHandler({ tabId: 7 });
      expect(lastIconColor()).toBe(GRAY);
    });
  });

  describe('refresh fallbacks (gray)', () => {
    test('non-http URL → gray, no native query', () => {
      mockTabsGet.mockImplementation((_tabId, cb) => cb({ url: 'chrome://newtab/' }));
      tabActivatedHandler({ tabId: 9 });
      expect(mockQueryNativeMessage).not.toHaveBeenCalled();
      expect(lastIconColor()).toBe(GRAY);
    });

    test('empty URL → gray, no native query', () => {
      mockTabsGet.mockImplementation((_tabId, cb) => cb({ url: '' }));
      tabActivatedHandler({ tabId: 9 });
      expect(mockQueryNativeMessage).not.toHaveBeenCalled();
      expect(lastIconColor()).toBe(GRAY);
    });

    test('native query lastError → gray', () => {
      mockTabsGet.mockImplementation((_tabId, cb) => cb({ url: URL }));
      mockQueryNativeMessage.mockImplementation((_host, _msg, cb) => {
        global.chrome.runtime.lastError = { message: 'host crashed' };
        cb(undefined);
        global.chrome.runtime.lastError = null;
      });
      tabActivatedHandler({ tabId: 9 });
      expect(lastIconColor()).toBe(GRAY);
    });

    test('native response with non-ok status → gray', () => {
      mockTabsGet.mockImplementation((_tabId, cb) => cb({ url: URL }));
      mockQueryNativeMessage.mockImplementation((_h, _m, cb) =>
        cb({ status: 'error', message: 'db locked' }),
      );
      tabActivatedHandler({ tabId: 9 });
      expect(lastIconColor()).toBe(GRAY);
    });
  });

  describe('tabs.onActivated guards', () => {
    test('tabs.get lastError → no setIcon call', () => {
      mockTabsGet.mockImplementation((_tabId, cb) => {
        global.chrome.runtime.lastError = { message: 'No tab with id' };
        cb(null);
        global.chrome.runtime.lastError = null;
      });
      tabActivatedHandler({ tabId: 99 });
      expect(mockSetIcon).not.toHaveBeenCalled();
    });

    test('tabs.get null tab without lastError → no setIcon call', () => {
      mockTabsGet.mockImplementation((_tabId, cb) => cb(null));
      tabActivatedHandler({ tabId: 99 });
      expect(mockSetIcon).not.toHaveBeenCalled();
    });
  });

  describe('webNavigation.onCompleted refreshes the icon', () => {
    test('http navigation triggers a query and a setIcon for that tabId', () => {
      mockTabsGet.mockImplementation((_tabId, cb) => cb({ title: 'Example Domain' }));
      respondWith({ read: [{ timestamp: 't' }], skimmed: [], of_interest: null });

      navHandler({ frameId: 0, tabId: 3, url: URL });

      expect(mockQueryNativeMessage).toHaveBeenCalledWith(
        'com.browser.visit.logger',
        { action: 'query', url: URL },
        expect.any(Function),
      );
      expect(lastIconColor()).toBe(GREEN);
      expect(lastIconTabId()).toBe(3);
    });

    test('iframe navigation does not refresh the icon', () => {
      navHandler({ frameId: 1, tabId: 3, url: URL });
      expect(mockSetIcon).not.toHaveBeenCalled();
      expect(mockQueryNativeMessage).not.toHaveBeenCalled();
    });
  });

  describe('refresh-icon message from popup', () => {
    test('triggers a query + setIcon for the supplied tab', () => {
      respondWith({ read: [], skimmed: [{ timestamp: 't' }], of_interest: null });
      messageHandler({ type: 'refresh-icon', tabId: 12, url: URL }, {}, jest.fn());
      expect(mockQueryNativeMessage).toHaveBeenCalledTimes(1);
      expect(lastIconColor()).toBe(YELLOW);
      expect(lastIconTabId()).toBe(12);
    });

    test('returns false (synchronous handler, no async response)', () => {
      respondWith(null);
      const result = messageHandler({ type: 'refresh-icon', tabId: 1, url: URL }, {}, jest.fn());
      expect(result).toBe(false);
    });
  });

  describe('icon glyph', () => {
    test('draws a white "B" centered on the colored disk', () => {
      mockTabsGet.mockImplementation((_tabId, cb) => cb({ url: URL }));
      respondWith({ read: [{ timestamp: 't' }], skimmed: [], of_interest: null });
      tabActivatedHandler({ tabId: 1 });

      const { imageData } = mockSetIcon.mock.calls[mockSetIcon.mock.calls.length - 1][0];
      expect(imageData[16].glyph).toEqual({ text: 'B', fillStyle: '#ffffff' });
      expect(imageData[32].glyph).toEqual({ text: 'B', fillStyle: '#ffffff' });
      // Disk color is the priority pick (green) — separate from the glyph fill.
      expect(imageData[16].color).toBe(GREEN);
    });
  });

  describe('icon image data caching', () => {
    test('reusing the same color does not redraw the canvas', () => {
      mockTabsGet.mockImplementation((_tabId, cb) => cb({ url: URL }));
      respondWith({ read: [{ timestamp: 't' }], skimmed: [], of_interest: null });

      tabActivatedHandler({ tabId: 1 });
      const drawsAfterFirst = global.OffscreenCanvas.mock.calls.length;
      tabActivatedHandler({ tabId: 2 });
      const drawsAfterSecond = global.OffscreenCanvas.mock.calls.length;

      // First call drew once per ICON_SIZES; second call (same green) is cached.
      expect(drawsAfterFirst).toBeGreaterThan(0);
      expect(drawsAfterSecond).toBe(drawsAfterFirst);
    });
  });

  describe('setIcon callback swallows lastError', () => {
    test('lastError after setIcon does not throw', () => {
      mockTabsGet.mockImplementation((_tabId, cb) => cb({ url: URL }));
      respondWith(null);
      mockSetIcon.mockImplementation((_arg, cb) => {
        global.chrome.runtime.lastError = { message: 'No tab with id 7' };
        cb();
        global.chrome.runtime.lastError = null;
      });
      expect(() => tabActivatedHandler({ tabId: 7 })).not.toThrow();
    });
  });
});
