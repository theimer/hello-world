'use strict';

const NATIVE_HOST = 'com.browser.visit.logger';

function showStatus(text) {
  document.getElementById('status').textContent = text;
}

document.addEventListener('DOMContentLoaded', () => {
  document.querySelectorAll('[data-tag]').forEach((btn) => {
    btn.addEventListener('click', () => {
      btn.disabled = true;
      document.querySelectorAll('[data-tag]').forEach(b => { b.disabled = true; });

      chrome.tabs.query({ active: true, currentWindow: true }, ([tab]) => {
        if (!tab) {
          showStatus('No active tab found.');
          return;
        }

        const timestamp = new Date().toISOString();
        const tag       = btn.dataset.tag;

        function handleResponse(response) {
          if (chrome.runtime.lastError) {
            showStatus('Error: ' + chrome.runtime.lastError.message);
            document.querySelectorAll('[data-tag]').forEach(b => { b.disabled = false; });
          } else if (response && response.status === 'ok') {
            window.close();
          } else {
            showStatus(response && response.message ? response.message : 'Write failed — check host log.');
            document.querySelectorAll('[data-tag]').forEach(b => { b.disabled = false; });
          }
        }

        if (tag === 'read') {
          showStatus('Saving snapshot\u2026');
          chrome.runtime.sendMessage({
            type:      'read-and-snapshot',
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
  });
});
