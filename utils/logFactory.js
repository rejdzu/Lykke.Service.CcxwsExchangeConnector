const os = require("os");
const winston = require('winston');
const { createLogger, format, transports, config } = winston;
const { combine, timestamp, label, printf } = format;

class LogFactory {

    static create(context, level) {

        const lykkeFormat = printf(x => {
            x.level = x.level.replace('error', 'ERROR').replace('warn', 'WARN').replace('info', 'INFO').replace('debug', 'DEBUG')
            const formatted = `${x.level} : ${x.timestamp} : ${x.label}${os.EOL}\t${x.message}`
            return formatted
        });

        const options = {
            transports: [
                new transports.Console({
                    label: context,
                    level: level,
                    format: combine(
                        label({ label: context }),
                        timestamp(),
                        format.colorize(),
                        lykkeFormat
                    ),
                    prettyPrint: true,
                    colorize: true,
                    //handleExceptions: true // doesn't work with unhandledRejection yet
                })
            ],
            exitOnError: false
        }

        winston.configure(options);

        const logger = createLogger(options);

        return logger
    }
}

module.exports = LogFactory