// Copyright (c) Lykke Corp.
// See the LICENSE file in the project root for more information.

const path = require('path');
const LogFactory = require('./utils/logFactory');

class SocketPublisher {

    constructor(socket, settings) {
        this._socket = socket;
        this._log = LogFactory.create(path.basename(__filename), settings.Main.LoggingLevel);
    }

    async publishBidAsk(tick) {
        if (this._socket.connecting || !this._socket.writable) {
            return;
        }

        this._socket.write(this._createBinaryFrame("order", tick));
    }

    async publishTrade(trade) {
        if (this._socket.connecting || !this._socket.writable) {
            return;
        }

        this._socket.write(this._createBinaryFrame("trade", trade));
    }

    _createFrame(type, data) {
        const obj = {
            type: type,
            data: data
        }

        return `${JSON.stringify(obj)}\n`
    }

    _createBinaryFrame(type, data) {
        const obj = {
            type: type,
            data: data
        }

        const payload = JSON.stringify(obj)
        const payloadLength = Buffer.byteLength(payload)
        const frameLength = payloadLength + 4

        const buffer = Buffer.alloc(frameLength)

        var offset = buffer.writeInt32BE(payloadLength, 0)
        buffer.write(payload, offset)

        return buffer
    }
}

module.exports = SocketPublisher;
