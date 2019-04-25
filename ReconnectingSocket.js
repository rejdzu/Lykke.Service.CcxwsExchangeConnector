// Copyright (c) Lykke Corp.
// See the LICENSE file in the project root for more information.

const net = require('net');
const path = require('path');
const LogFactory = require('./utils/logFactory');

class ReconnectingSocket {

    constructor(port, host, reconnectionIntervalMs, timeoutMs, loggingLevel) {
        this._host = host;
        this._port = port;
        this._socket = undefined;
        this._log = LogFactory.create(path.basename(__filename), loggingLevel);
        this._reconnectionIntervalMs = reconnectionIntervalMs;
        this._timeoutMs = timeoutMs;

        this.reconnecting = false;
        this.connecting = false;
        this.writable = false;
    }

    async setup() {
        let setupConnection = () => new Promise((resolve, reject) => {
            let socket = new net.Socket();

            this._log.info("Connecting...");
            socket.connect(this._port, this._host, () => {
                this._log.info('Socket connected on: ' + this._host + ':' + this._port);

                this.reconnecting = false;
                this.connecting = false;
                this.writable = socket.writable;

                if (this._timeoutMs) {
                    socket.setTimeout(this._timeoutMs);
                }

                resolve(socket);
            });
            this.connecting = socket.connecting;

            let reconnect = () => {
                if (!this.reconnecting) {
                    this.reconnecting = true;
                    this._log.info(`Trying to reconnect in ${this._reconnectionIntervalMs}ms`);
                    setTimeout(async () => this.setup(), this._reconnectionIntervalMs);
                }
            };

            socket.on('close', () => {
                this._log.info(`Socket closed`);
                reconnect();
            });

            socket.on('error', (error) => {
                this._log.warn(`Socket error: ${error.message}`);
                this.reconnecting = false;
            });

            socket.on('timeout', () => {
                this._log.warn(`Socket connection timed out.`);
                socket.destroy();
                reconnect();
            });
        });

        this._socket = await setupConnection();
    }

    write(data) {
        if (!this._socket.writable) {
            return
        }
        this._socket.write(data, (err) => {
            if (err) {
                this._log.warn(`Error writing to socket: ${err}`);
            }
        });
    }
}

module.exports = ReconnectingSocket;
