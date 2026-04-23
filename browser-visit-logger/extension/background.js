'use strict';

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
    title:     isTitleMeaningful(entry.title, entry.url) ? entry.title : entry.url,
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

  const entry = pendingVisits.get(tabId);
  if (!isTitleMeaningful(changeInfo.title, entry.url)) return;

  entry.title = changeInfo.title;
  flushVisit(tabId);
});

// Handle "read" tag from popup: capture MHTML snapshot, download it, then tag via native host.
chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
  if (msg.type !== 'read-and-snapshot') return false;

  const { tabId, timestamp, url, title } = msg;

  chrome.pageCapture.saveAsMHTML({ tabId }, (mhtmlData) => {
    if (chrome.runtime.lastError || !mhtmlData) {
      sendResponse({
        status: 'error',
        message: 'Snapshot capture failed: ' + (chrome.runtime.lastError?.message || 'no data'),
      });
      return;
    }

    const blobUrl = URL.createObjectURL(mhtmlData);
    const tmpFilename = `bvl-snapshot-${Date.now()}.mhtml`;

    chrome.downloads.download({ url: blobUrl, filename: tmpFilename, saveAs: false }, (downloadId) => {
      URL.revokeObjectURL(blobUrl);

      if (chrome.runtime.lastError || downloadId === undefined) {
        sendResponse({
          status: 'error',
          message: 'Snapshot download failed: ' + (chrome.runtime.lastError?.message || 'unknown'),
        });
        return;
      }

      const onChanged = (delta) => {
        if (delta.id !== downloadId) return;

        if (delta.state?.current === 'complete') {
          chrome.downloads.onChanged.removeListener(onChanged);
          chrome.downloads.search({ id: downloadId }, ([item]) => {
            chrome.runtime.sendNativeMessage(NATIVE_HOST, {
              timestamp, url, title,
              tag: 'read',
              snapshot_download_path: item.filename,
            }, (response) => {
              if (chrome.runtime.lastError) {
                sendResponse({ status: 'error', message: chrome.runtime.lastError.message });
              } else {
                sendResponse(response);
              }
            });
          });
        } else if (delta.state?.current === 'interrupted') {
          chrome.downloads.onChanged.removeListener(onChanged);
          sendResponse({ status: 'error', message: 'Snapshot download was interrupted' });
        }
      };

      chrome.downloads.onChanged.addListener(onChanged);
    });
  });

  return true; // Keep message channel open for async response
});
