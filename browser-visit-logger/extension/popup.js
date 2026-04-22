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
        const message = {
          timestamp: new Date().toISOString(),
          url:       tab.url,
          title:     tab.title || tab.url,
          tag:       btn.dataset.tag,
        };
        chrome.runtime.sendNativeMessage(NATIVE_HOST, message, (response) => {
          if (chrome.runtime.lastError) {
            showStatus('Error: ' + chrome.runtime.lastError.message);
            document.querySelectorAll('[data-tag]').forEach(b => { b.disabled = false; });
          } else if (response && response.status === 'ok') {
            window.close();
          } else {
            showStatus(response && response.message ? response.message : 'Write failed — check host log.');
            document.querySelectorAll('[data-tag]').forEach(b => { b.disabled = false; });
          }
        });
      });
    });
  });
});
