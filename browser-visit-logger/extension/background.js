const NATIVE_HOST = 'com.browser.visit.logger';
const TITLE_FLUSH_TIMEOUT_MS = 5000;

// tabId -> { url, title, timestamp, timerId }
const pendingVisits = new Map();

function isTitleMeaningful(title, url) {
  if (!title || title.trim() === '') return false;
  try {
    const u = new URL(url);
    if (title === u.hostname) return false;
    if (title === url) return false;
  } catch (_) {}
  return true;
}

function flushVisit(tabId) {
  const entry = pendingVisits.get(tabId);
  if (!entry) return;
  pendingVisits.delete(tabId);
  clearTimeout(entry.timerId);

  const payload = {
    timestamp: entry.timestamp,
    url:       entry.url,
    title:     entry.title || entry.url,
  };

  chrome.runtime.sendNativeMessage(NATIVE_HOST, payload, (response) => {
    if (chrome.runtime.lastError) {
      console.error('[BVL] Native message error:', chrome.runtime.lastError.message);
    }
  });
}

chrome.webNavigation.onCompleted.addListener((details) => {
  // Only log main frame navigations, not iframes
  if (details.frameId !== 0) return;

  const timestamp = new Date().toISOString();

  chrome.tabs.get(details.tabId, (tab) => {
    if (chrome.runtime.lastError) return;

    const title = (tab && tab.title) ? tab.title : '';

    // If the title is already meaningful, send immediately
    if (isTitleMeaningful(title, details.url)) {
      chrome.runtime.sendNativeMessage(NATIVE_HOST, { timestamp, url: details.url, title }, (response) => {
        if (chrome.runtime.lastError) {
          console.error('[BVL] Native message error:', chrome.runtime.lastError.message);
        }
      });
      return;
    }

    // Park the entry and wait for the title to arrive or the timeout to fire
    if (pendingVisits.has(details.tabId)) {
      clearTimeout(pendingVisits.get(details.tabId).timerId);
    }

    const timerId = setTimeout(() => flushVisit(details.tabId), TITLE_FLUSH_TIMEOUT_MS);
    pendingVisits.set(details.tabId, { url: details.url, title, timestamp, timerId });
  });
});

chrome.tabs.onUpdated.addListener((tabId, changeInfo) => {
  if (!changeInfo.title) return;
  if (!pendingVisits.has(tabId)) return;

  pendingVisits.get(tabId).title = changeInfo.title;
  flushVisit(tabId);
});
