package com.p6spy.engine.spy.appender;

import java.time.LocalDateTime;

/**
 * 优化sql输出格式，采用hibernate的 Formatter
 */
public class PrettySqlMultiLineFormat implements MessageFormattingStrategy {

    /**
     * Formats a log message for the logging module
     *
     * @param connectionId the id of the connection
     * @param now          the current ime expressing in milliseconds
     * @param elapsed      the time in milliseconds that the operation took to complete
     * @param category     the category of the operation
     * @param prepared     the SQL statement with all bind variables replaced with actual values
     * @param sql          the sql statement executed
     * @return the formatted log message
     */

    @Override
    public String formatMessage(int connectionId, String now, long elapsed, String category, String prepared, String sql) {
        return !"".equals(sql.trim()) ? "[ " + LocalDateTime.now() + " ] --- | took "
                + elapsed + "ms | " + category + " | connection " + connectionId + "|\t "
                + sql + ";" : "";
    }
}