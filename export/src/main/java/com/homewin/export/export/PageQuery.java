package com.homewin.export.export;

import java.util.List;

/**
 * @description:
 * @author: homewin
 * @create: 2023-06-29 15:26
 **/
public interface PageQuery<T> {
    /**
     * 分段
     *
     * @param start 分页开始
     * @param end   分页结束
     * @return T
     */
    List<T> page(int start, int end);
}
