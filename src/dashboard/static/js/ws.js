/**
 * DashboardWebSocket — 带自动重连的 WebSocket 客户端
 */
class DashboardWebSocket {
  constructor(url, opts = {}) {
    this.url = url;
    this.onOpen = opts.onOpen || (() => {});
    this.onClose = opts.onClose || (() => {});
    this.onMessage = opts.onMessage || (() => {});
    this.ws = null;
    this._retryDelay = 1000;
    this._maxRetry = 30000;
    this._timer = null;
    this._pingTimer = null;
    this._destroyed = false;
  }

  connect() {
    if (this._destroyed) return;
    try {
      this.ws = new WebSocket(this.url);
    } catch (e) {
      this._scheduleReconnect();
      return;
    }

    this.ws.onopen = () => {
      this._retryDelay = 1000;
      this._startPing();
      this.onOpen();
    };

    this.ws.onclose = () => {
      this._stopPing();
      this.onClose();
      this._scheduleReconnect();
    };

    this.ws.onerror = () => {};

    this.ws.onmessage = (evt) => {
      try {
        const msg = JSON.parse(evt.data);
        this.onMessage(msg);
      } catch (e) {
        console.warn('[WS] parse error:', e);
      }
    };
  }

  send(data) {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(typeof data === 'string' ? data : JSON.stringify(data));
    }
  }

  subscribe(channels) {
    this.send({ action: 'subscribe', channels });
  }

  unsubscribe(channels) {
    this.send({ action: 'unsubscribe', channels });
  }

  destroy() {
    this._destroyed = true;
    this._stopPing();
    clearTimeout(this._timer);
    if (this.ws) this.ws.close();
  }

  _startPing() {
    this._stopPing();
    this._pingTimer = setInterval(() => {
      this.send({ action: 'ping' });
    }, 15000);  // 每15秒发送ping保活
  }

  _stopPing() {
    if (this._pingTimer) {
      clearInterval(this._pingTimer);
      this._pingTimer = null;
    }
  }

  _scheduleReconnect() {
    if (this._destroyed) return;
    this._timer = setTimeout(() => {
      this._retryDelay = Math.min(this._retryDelay * 1.5, this._maxRetry);
      this.connect();
    }, this._retryDelay);
  }
}
