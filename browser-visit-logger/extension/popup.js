'use strict';

const NATIVE_HOST = 'com.browser.visit.logger';

function showStatus(text) {
  document.getElementById('status').textContent = text;
}

/**
 * Format an ISO timestamp into a short human-readable string, e.g.
 * "Apr 23, 2026, 10:04 AM".  Returns null for falsy input.
 */
function formatTs(iso) {
  if (!iso) return null;
  return new Date(iso).toLocaleString(undefined, {
    year: 'numeric', month: 'short', day: 'numeric',
    hour: '2-digit', minute: '2-digit',
  });
}

/**
 * Render the visit history section from the record returned by the host.
 * Does nothing if record is null (URL not yet in the database).
 */
function showVisitInfo(record) {
  if (!record) return;

  const rows = [
    { label: 'First visited', value: formatTs(record.timestamp) },
    { label: '★ Of Interest', value: record.of_interest ? '' : null },
    ...(record.read || []).map((r) => ({ label: '✓ Read', value: formatTs(r.timestamp) })),
    ...(record.skimmed || []).map((r) => ({ label: '~ Skimmed', value: formatTs(r.timestamp) })),
  ].filter((r) => r.value !== null);

  if (rows.length === 0) return;

  document.getElementById('visit-rows').innerHTML = rows
    .map((r) =>
      `<div class="info-row">` +
        `<span class="info-label">${r.label}</span>` +
        (r.value ? `<span class="info-value">${r.value}</span>` : '') +
      `</div>`
    )
    .join('');

  document.getElementById('visit-info').style.display = 'block';
}

document.addEventListener('DOMContentLoaded', () => {
  chrome.tabs.query({ active: true, currentWindow: true }, ([tab]) => {
    if (!tab) {
      showStatus('No active tab found.');
      return;
    }

    // Query the native host for any existing record for this URL.
    chrome.runtime.sendNativeMessage(NATIVE_HOST, { action: 'query', url: tab.url },
      (response) => {
        // Ignore errors — the history section simply stays hidden.
        if (!chrome.runtime.lastError && response && response.status === 'ok') {
          showVisitInfo(response.record);
        }

        // Set up tag buttons after the query returns (so the popup doesn't
        // flash in if the query is fast).
        setupButtons(tab);
      }
    );
  });
});

function setupButtons(tab) {
  document.querySelectorAll('[data-tag]').forEach((btn) => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('[data-tag]').forEach(b => { b.disabled = true; });

      const timestamp = new Date().toISOString();
      const tag       = btn.dataset.tag;

      function handleResponse(response) {
        if (chrome.runtime.lastError) {
          showStatus('Error: ' + chrome.runtime.lastError.message);
          document.querySelectorAll('[data-tag]').forEach(b => { b.disabled = false; });
        } else if (response && response.status === 'ok') {
          chrome.runtime.sendMessage({ type: 'refresh-icon', tabId: tab.id, url: tab.url });
          window.close();
        } else {
          showStatus(response && response.message ? response.message : 'Write failed — check host log.');
          document.querySelectorAll('[data-tag]').forEach(b => { b.disabled = false; });
        }
      }

      if (tag === 'read' || tag === 'skimmed') {
        showStatus('Saving snapshot\u2026');
        chrome.runtime.sendMessage({
          type:      'tag-and-snapshot',
          tag,
          tabId:     tab.id,
          timestamp,
          url:       tab.url,
          title:     tab.title || tab.url,
        }, handleResponse);
      } else {
        chrome.runtime.sendNativeMessage(NATIVE_HOST, {
          timestamp,
          url:   tab.url,
          title: tab.title || tab.url,
          tag,
        }, handleResponse);
      }
    });
  });
}
