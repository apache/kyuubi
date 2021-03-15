package org.apache.kyuubi.web.utils;

public class PageUtils {

    public static int pageIndex(Integer page) {
        if (page == null || page < 1) {
            return 1;
        } else {
            return page;
        }
    }

    public static int pageSize(Integer pageSize) {
        if (pageSize == null || pageSize < 1) {
            return 20;
        } else {
            return Math.min(pageSize, 1000);
        }
    }

    public static String limitSQL(int pageIndex, int pageSize) {
        long skip = 1L * (pageIndex - 1) * pageSize;
        return " limit " + skip + "," + pageSize;
    }

    public static int calcTotalPage(long totalCount, int realPageSize) {
        if (totalCount % realPageSize == 0L) {
            return (int) (totalCount / realPageSize);
        } else {
            return (int) ((totalCount / realPageSize) + 1);
        }
    }
}
