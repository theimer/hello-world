/**
 * @jest-environment jsdom
 */
'use strict';
/**
 * Unit tests for extension/popup.js
 *
 * Run with:
 *   cd browser-visit-logger
 *   npm test
 *
 * Chrome extension APIs and window.close are mocked below — no browser required.
 * jsdom provides the DOM environment (document, Event, etc.).
 */

// ---------------------------------------------------------------------------
// Chrome API mocks
// ---------------------------------------------------------------------------
const mockTabsQuery         = jest.fn();
const mockSendNativeMessage = jest.fn();
const mockSendMessage       = jest.fn();

function buildChromeMock() {
  global.chrome = {
    tabs: {
      query: mockTabsQuery,
    },
    runtime: {
      sendNativeMessage: mockSendNativeMessage,
      sendMessage:       mockSendMessage,
      lastError:         null,
    },
  };
}

// ---------------------------------------------------------------------------
// DOM reset — recreate the popup HTML structure before each test
// ---------------------------------------------------------------------------
function setupDOM() {
  document.body.innerHTML = `
    <div id="visit-info">
      <div id="visit-rows"></div>
      <hr>
    </div>
    <h3>Mark this page</h3>
    <button data-tag="of_interest">&#9733; Of Interest</button>
    <button data-tag="read">&#10003; Read</button>
    <button data-tag="skimmed">&#126; Skimmed</button>
    <div id="status"></div>
  `;
  // Mirror the CSS default: #visit-info starts hidden via inline style
  document.getElementById('visit-info').style.display = 'none';
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/**
 * Load a fresh copy of popup.js and invoke its DOMContentLoaded callback.
 *
 * We intercept document.addEventListener so the callback is captured and
 * called directly — this prevents listeners from accumulating on the document
 * across tests when loadPopup() is called multiple times in a test file.
 */
async function loadPopup() {
  jest.resetModules();

  // Capture the DOMContentLoaded handler without actually registering it
  let domContentLoadedCb = null;
  const origAddEventListener = document.addEventListener.bind(document);
  jest.spyOn(document, 'addEventListener').mockImplementation((event, cb, ...args) => {
    if (event === 'DOMContentLoaded') {
      domContentLoadedCb = cb;
    } else {
      origAddEventListener(event, cb, ...args);
    }
  });

  require('../extension/popup');
  document.addEventListener.mockRestore();

  if (domContentLoadedCb) await domContentLoadedCb();

  // Flush microtasks so native-message callbacks complete
  await Promise.resolve();
  await Promise.resolve();
}

/**
 * Make chrome.tabs.query call its callback with the given tab (or no tab).
 */
function tabReturns(tab) {
  mockTabsQuery.mockImplementation((_query, cb) => cb(tab ? [tab] : []));
}

/**
 * Make chrome.runtime.sendNativeMessage call its callback with the given
 * response, optionally simulating a lastError.
 */
function nativeReturns(response, { lastError = null } = {}) {
  mockSendNativeMessage.mockImplementation((_host, _msg, cb) => {
    global.chrome.runtime.lastError = lastError;
    cb(response);
    global.chrome.runtime.lastError = null;
  });
}

// ---------------------------------------------------------------------------
// beforeEach / afterEach
// ---------------------------------------------------------------------------
beforeEach(() => {
  setupDOM();
  buildChromeMock();
  window.close = jest.fn();
  mockTabsQuery.mockReset();
  mockSendNativeMessage.mockReset();
  mockSendMessage.mockReset();
});

// ---------------------------------------------------------------------------
// formatTs — tested indirectly through what showVisitInfo renders
// ---------------------------------------------------------------------------
describe('formatTs', () => {
  const TAB = { id: 1, url: 'https://example.com/', title: 'Example' };

  test('renders nothing when timestamp is null', async () => {
    nativeReturns({ status: 'ok', record: { timestamp: null, of_interest: null, read: [], skimmed: []} });
    tabReturns(TAB);
    await loadPopup();
    expect(document.querySelectorAll('.info-row').length).toBe(0);
  });

  test('renders a human-readable date string, not the raw ISO timestamp', async () => {
    const ISO = '2026-04-23T14:09:34.261Z';
    nativeReturns({ status: 'ok', record: { timestamp: ISO, of_interest: null, read: [], skimmed: []} });
    tabReturns(TAB);
    await loadPopup();
    const valueEl = document.querySelector('.info-value');
    expect(valueEl).not.toBeNull();
    // Must not be the raw ISO string — formatTs must have reformatted it
    expect(valueEl.textContent).not.toBe(ISO);
    // The day '23' from the timestamp must appear in the formatted output
    expect(valueEl.textContent).toContain('23');
  });

  test('formatted string includes the year for a 2026 timestamp', async () => {
    nativeReturns({ status: 'ok', record: { timestamp: '2026-04-23T14:09:34.261Z', of_interest: null, read: [], skimmed: []} });
    tabReturns(TAB);
    await loadPopup();
    const valueEl = document.querySelector('.info-value');
    expect(valueEl.textContent).toContain('2026');
  });
});

// ---------------------------------------------------------------------------
// showVisitInfo
// ---------------------------------------------------------------------------
describe('showVisitInfo', () => {
  const SNAP_DIR = '/Users/x/Downloads/browser-visit-snapshots';
  const RECORD = {
    timestamp: '2026-04-23T02:10:31.451Z',
    of_interest: true,
    read:    [{ timestamp: '2026-04-23T02:27:10.366Z', filename: 'abc.mhtml', directory: SNAP_DIR }],
    skimmed: [],
  };
  const TAB = { id: 1, url: 'https://example.com/', title: 'Example' };

  beforeEach(() => {
    tabReturns(TAB);
  });

  test('sets #visit-info display to "block" when record has data', async () => {
    nativeReturns({ status: 'ok', record: RECORD });
    await loadPopup();
    expect(document.getElementById('visit-info').style.display).toBe('block');
  });

  test('leaves #visit-info hidden when record is null', async () => {
    nativeReturns({ status: 'ok', record: null });
    await loadPopup();
    expect(document.getElementById('visit-info').style.display).toBe('none');
  });

  test('leaves #visit-info hidden when all timestamps are null', async () => {
    nativeReturns({ status: 'ok', record: { timestamp: null, of_interest: null, read: [], skimmed: []} });
    await loadPopup();
    expect(document.getElementById('visit-info').style.display).toBe('none');
  });

  test('renders a row for each non-null timestamp', async () => {
    nativeReturns({ status: 'ok', record: RECORD });
    await loadPopup();
    // RECORD: timestamp, of_interest, read[0] are set (3); skimmed is null
    expect(document.querySelectorAll('.info-row').length).toBe(3);
  });

  test('renders a separate row for each read event when read multiple times', async () => {
    const multiReadRecord = {
      ...RECORD,
      read: [
        { timestamp: '2026-04-23T02:27:10.366Z', filename: 'abc.mhtml', directory: SNAP_DIR },
        { timestamp: '2026-04-24T09:15:00.000Z', filename: 'def.mhtml', directory: SNAP_DIR },
      ],
    };
    nativeReturns({ status: 'ok', record: multiReadRecord });
    await loadPopup();
    // timestamp + of_interest + read[0] + read[1] = 4 rows
    expect(document.querySelectorAll('.info-row').length).toBe(4);
    const labels = [...document.querySelectorAll('.info-label')].map(el => el.textContent);
    expect(labels.filter(l => l === '✓ Read').length).toBe(2);
  });

  test('renders no read rows when read is an empty array', async () => {
    nativeReturns({ status: 'ok', record: { ...RECORD, read: [] } });
    await loadPopup();
    const labels = [...document.querySelectorAll('.info-label')].map(el => el.textContent);
    expect(labels).not.toContain('✓ Read');
  });

  test('handles record.read being null without throwing (defensive fallback)', async () => {
    // The || [] guard in showVisitInfo protects against null coming from old clients
    nativeReturns({ status: 'ok', record: { ...RECORD, read: null } });
    await loadPopup();
    const labels = [...document.querySelectorAll('.info-label')].map(el => el.textContent);
    expect(labels).not.toContain('✓ Read');
  });

  test('renders a separate row for each skimmed event when skimmed multiple times', async () => {
    const multiSkimRecord = {
      ...RECORD,
      skimmed: [
        { timestamp: '2026-04-23T02:27:10.366Z', filename: 's1.mhtml', directory: SNAP_DIR },
        { timestamp: '2026-04-24T09:15:00.000Z', filename: 's2.mhtml', directory: SNAP_DIR },
      ],
    };
    nativeReturns({ status: 'ok', record: multiSkimRecord });
    await loadPopup();
    // timestamp + of_interest + read[0] + skimmed[0] + skimmed[1] = 5 rows
    expect(document.querySelectorAll('.info-row').length).toBe(5);
    const labels = [...document.querySelectorAll('.info-label')].map(el => el.textContent);
    expect(labels.filter(l => l === '~ Skimmed').length).toBe(2);
  });

  test('renders no skimmed rows when skimmed is an empty array', async () => {
    nativeReturns({ status: 'ok', record: { ...RECORD, skimmed: [] } });
    await loadPopup();
    const labels = [...document.querySelectorAll('.info-label')].map(el => el.textContent);
    expect(labels).not.toContain('~ Skimmed');
  });

  test('handles record.skimmed being null without throwing (defensive fallback)', async () => {
    // The || [] guard in showVisitInfo protects against null coming from old clients
    nativeReturns({ status: 'ok', record: { ...RECORD, skimmed: null } });
    await loadPopup();
    const labels = [...document.querySelectorAll('.info-label')].map(el => el.textContent);
    expect(labels).not.toContain('~ Skimmed');
  });

  test('does not render a row for a null timestamp within an event', async () => {
    // If an event's timestamp is null, formatTs returns null and the row is filtered out.
    // This covers the `.filter((r) => r.value !== null)` branch for individual event rows.
    const nullTsRecord = {
      ...RECORD,
      skimmed: [{ timestamp: null, filename: 'x.mhtml', directory: SNAP_DIR }],
    };
    nativeReturns({ status: 'ok', record: nullTsRecord });
    await loadPopup();
    const labels = [...document.querySelectorAll('.info-label')].map(el => el.textContent);
    expect(labels).not.toContain('~ Skimmed');
  });

  test('renders the "First visited" row from record.timestamp', async () => {
    nativeReturns({ status: 'ok', record: RECORD });
    await loadPopup();
    const labels = [...document.querySelectorAll('.info-label')].map(el => el.textContent);
    expect(labels).toContain('First visited');
  });

  test('renders the "★ Of Interest" row when of_interest is set', async () => {
    nativeReturns({ status: 'ok', record: RECORD });
    await loadPopup();
    const labels = [...document.querySelectorAll('.info-label')].map(el => el.textContent);
    expect(labels).toContain('★ Of Interest');
  });

  test('renders the "✓ Read" row when read array has one entry', async () => {
    nativeReturns({ status: 'ok', record: RECORD });
    await loadPopup();
    const labels = [...document.querySelectorAll('.info-label')].map(el => el.textContent);
    expect(labels).toContain('✓ Read');
  });

  test('renders the "~ Skimmed" row when skimmed array has one entry', async () => {
    nativeReturns({ status: 'ok', record: { ...RECORD, skimmed: [{ timestamp: '2026-04-23T02:27:10.366Z', filename: 'sk.mhtml', directory: SNAP_DIR }] } });
    await loadPopup();
    const labels = [...document.querySelectorAll('.info-label')].map(el => el.textContent);
    expect(labels).toContain('~ Skimmed');
  });

  test('leaves #visit-info hidden when query returns an error status', async () => {
    nativeReturns({ status: 'error', message: 'not found' });
    await loadPopup();
    expect(document.getElementById('visit-info').style.display).toBe('none');
  });

  test('leaves #visit-info hidden when native messaging fails (lastError set)', async () => {
    mockSendNativeMessage.mockImplementation((_host, _msg, cb) => {
      global.chrome.runtime.lastError = { message: 'Host not found' };
      cb(null);
      global.chrome.runtime.lastError = null;
    });
    await loadPopup();
    expect(document.getElementById('visit-info').style.display).toBe('none');
  });
});

// ---------------------------------------------------------------------------
// DOMContentLoaded handler
// ---------------------------------------------------------------------------
describe('DOMContentLoaded handler', () => {
  const TAB = { id: 1, url: 'https://example.com/', title: 'Example' };

  test('calls chrome.tabs.query for the active tab in the current window', async () => {
    nativeReturns({ status: 'ok', record: null });
    tabReturns(TAB);
    await loadPopup();
    expect(mockTabsQuery).toHaveBeenCalledWith(
      { active: true, currentWindow: true },
      expect.any(Function),
    );
  });

  test('shows "No active tab found." when tabs.query returns no tab', async () => {
    tabReturns(null);
    await loadPopup();
    expect(document.getElementById('status').textContent).toBe('No active tab found.');
    expect(mockSendNativeMessage).not.toHaveBeenCalled();
  });

  test('sends a query action to the native host with the tab URL', async () => {
    nativeReturns({ status: 'ok', record: null });
    tabReturns(TAB);
    await loadPopup();
    expect(mockSendNativeMessage).toHaveBeenCalledWith(
      'com.browser.visit.logger',
      { action: 'query', url: TAB.url },
      expect.any(Function),
    );
  });

  test('renders visit info from the host response', async () => {
    nativeReturns({ status: 'ok', record: {
      timestamp: '2026-04-23T02:10:31.451Z',
      of_interest: null, read: [], skimmed: [],
    }});
    tabReturns(TAB);
    await loadPopup();
    expect(document.getElementById('visit-info').style.display).toBe('block');
  });

  test('sets up tag buttons even when the query fails', async () => {
    mockSendNativeMessage.mockImplementation((_host, _msg, cb) => {
      global.chrome.runtime.lastError = { message: 'Host not found' };
      cb(null);
      global.chrome.runtime.lastError = null;
    });
    tabReturns(TAB);
    await loadPopup();
    // Buttons should be present and clickable (not disabled)
    const buttons = document.querySelectorAll('[data-tag]');
    expect(buttons.length).toBe(3);
    buttons.forEach(btn => expect(btn.disabled).toBe(false));
  });
});

// ---------------------------------------------------------------------------
// setupButtons — tag button click handlers
// ---------------------------------------------------------------------------
describe('setupButtons', () => {
  const TAB = { id: 1, url: 'https://example.com/', title: 'Example Page' };

  beforeEach(() => {
    // Default: query succeeds with no record (so setupButtons is always called)
    tabReturns(TAB);
    nativeReturns({ status: 'ok', record: null });
  });

  /**
   * Load popup (query succeeds), then reset the mock and click a tag button.
   * Any subsequent mock behaviour should be set up before calling this.
   */
  async function clickTag(tag) {
    await loadPopup();
    mockSendNativeMessage.mockClear();
    document.querySelector(`[data-tag="${tag}"]`).click();
  }

  test('clicking "of_interest" sends a native message with tag "of_interest"', async () => {
    nativeReturns({ status: 'ok', record: null });
    await clickTag('of_interest');
    expect(mockSendNativeMessage).toHaveBeenCalledWith(
      'com.browser.visit.logger',
      expect.objectContaining({ tag: 'of_interest', url: TAB.url, title: TAB.title }),
      expect.any(Function),
    );
  });

  test('clicking "skimmed" sends runtime.sendMessage (not sendNativeMessage) for snapshot', async () => {
    await loadPopup();
    mockSendNativeMessage.mockClear();
    mockSendMessage.mockClear();
    document.querySelector('[data-tag="skimmed"]').click();
    expect(mockSendMessage).toHaveBeenCalledWith(
      expect.objectContaining({ type: 'tag-and-snapshot', tag: 'skimmed', url: TAB.url, tabId: TAB.id }),
      expect.any(Function),
    );
    expect(mockSendNativeMessage).not.toHaveBeenCalled();
  });

  test('tag message includes a valid ISO timestamp', async () => {
    nativeReturns({ status: 'ok', record: null });
    await clickTag('of_interest');
    const msg = mockSendNativeMessage.mock.calls[0][1];
    const parsed = new Date(msg.timestamp);
    expect(isNaN(parsed.getTime())).toBe(false);
    expect(parsed.getFullYear()).toBeGreaterThan(2000);
  });

  test('clicking "read" sends runtime.sendMessage (not sendNativeMessage) for snapshot', async () => {
    await loadPopup();
    mockSendNativeMessage.mockClear();
    mockSendMessage.mockClear();
    document.querySelector('[data-tag="read"]').click();
    expect(mockSendMessage).toHaveBeenCalledWith(
      expect.objectContaining({ type: 'tag-and-snapshot', tag: 'read', url: TAB.url, tabId: TAB.id }),
      expect.any(Function),
    );
    expect(mockSendNativeMessage).not.toHaveBeenCalled();
  });

  test('buttons are disabled while a request is in flight', async () => {
    // Load popup with the default (ok) query so setupButtons is called
    await loadPopup();
    // Override mock so the tag-click native message never calls back
    mockSendNativeMessage.mockImplementation(() => {});
    document.querySelector('[data-tag="of_interest"]').click();
    document.querySelectorAll('[data-tag]').forEach(btn => {
      expect(btn.disabled).toBe(true);
    });
  });

  test('on success, window.close() is called', async () => {
    // Set the mock so the tag-click returns ok before loadPopup so both
    // the query and the tag response return ok (record null is fine)
    nativeReturns({ status: 'ok', record: null });
    await clickTag('of_interest');
    expect(window.close).toHaveBeenCalled();
  });

  test('on error response, shows the error message and re-enables buttons', async () => {
    await loadPopup();
    mockSendNativeMessage.mockImplementation((_host, _msg, cb) =>
      cb({ status: 'error', message: 'No record found for this URL — visit the page before tagging' })
    );
    mockSendNativeMessage.mockClear();
    document.querySelector('[data-tag="of_interest"]').click();
    expect(document.getElementById('status').textContent).toBe(
      'No record found for this URL — visit the page before tagging'
    );
    document.querySelectorAll('[data-tag]').forEach(btn => {
      expect(btn.disabled).toBe(false);
    });
  });

  test('on native messaging error (lastError), shows error text and re-enables buttons', async () => {
    await loadPopup();
    mockSendNativeMessage.mockImplementation((_host, _msg, cb) => {
      global.chrome.runtime.lastError = { message: 'Host not found' };
      cb(null);
      global.chrome.runtime.lastError = null;
    });
    mockSendNativeMessage.mockClear();
    document.querySelector('[data-tag="of_interest"]').click();
    expect(document.getElementById('status').textContent).toContain('Error:');
    document.querySelectorAll('[data-tag]').forEach(btn => {
      expect(btn.disabled).toBe(false);
    });
  });

  test('uses tab.url as title fallback for "read" when tab.title is empty', async () => {
    tabReturns({ id: 1, url: 'https://example.com/', title: '' });
    nativeReturns({ status: 'ok', record: null });
    await loadPopup();
    mockSendMessage.mockClear();
    document.querySelector('[data-tag="read"]').click();
    expect(mockSendMessage).toHaveBeenCalledWith(
      expect.objectContaining({ title: 'https://example.com/' }),
      expect.any(Function),
    );
  });

  test('uses tab.url as title fallback for "of_interest" when tab.title is empty', async () => {
    tabReturns({ id: 1, url: 'https://example.com/', title: '' });
    nativeReturns({ status: 'ok', record: null });
    await loadPopup();
    mockSendNativeMessage.mockClear();
    document.querySelector('[data-tag="of_interest"]').click();
    expect(mockSendNativeMessage).toHaveBeenCalledWith(
      'com.browser.visit.logger',
      expect.objectContaining({ title: 'https://example.com/' }),
      expect.any(Function),
    );
  });

  test('on error response with no message field, shows fallback text', async () => {
    await loadPopup();
    mockSendNativeMessage.mockImplementation((_host, _msg, cb) => cb({ status: 'error' }));
    mockSendNativeMessage.mockClear();
    document.querySelector('[data-tag="of_interest"]').click();
    expect(document.getElementById('status').textContent).toBe(
      'Write failed — check host log.'
    );
  });
});
