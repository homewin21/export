package com.homewin.export.export;

import com.alibaba.excel.EasyExcel;
import com.alibaba.excel.ExcelWriter;
import com.alibaba.excel.write.metadata.WriteSheet;
import com.cmcc.calc.mapper.SopRelativesLoyalCustomersMapper;
import jakarta.annotation.Resource;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.poi.ss.SpreadsheetVersion;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @description:
 * @author: homewin
 * @create: 2023-06-28 16:40
 **/
@Slf4j
@Component
public class ExportInvoker<T> {
    @Resource(name = "sopAsyncExecutor")
    private Executor pool;
    /**
     * 阻塞队列的预设长度
     */
    private final int CAPACITY = 1000000;
    /**
     * 标识插入的对象类型
     */
    private T data;
    /**
     * 默认的读任务并发数量
     */
    private final int readNum = 20;

    public void setData(T data) {
        this.data = data;
    }


    public void export(PageQuery<T> pageQuery, String fileName, Integer readNum, Integer count, String executionId) {
        if (readNum == null) {
            readNum = this.readNum;
        }
        AtomicInteger begin = new AtomicInteger(0);
        AtomicInteger end = new AtomicInteger(count);
        CountDownLatch read = new CountDownLatch(readNum);
        CountDownLatch write = new CountDownLatch(1);
        BlockingQueue<T> queue = new ArrayBlockingQueue<>(CAPACITY);
        ExportWriteThread writeThread = new ExportWriteThread(fileName, read, write, queue, executionId);
        pool.execute(writeThread);
        for (int i = 0; i < readNum; i++) {
            ExportReadThread readThread = new ExportReadThread(pageQuery, begin, end, read, queue);
            pool.execute(readThread);
        }


    }


    class ExportWriteThread implements Runnable {
        private final String fileName;
        private final CountDownLatch read;
        private final CountDownLatch write;
        private final BlockingQueue<T> deque;
        private final String executionId;

        public ExportWriteThread(String fileName, CountDownLatch read, CountDownLatch write, BlockingQueue<T> deque, String executionId) {
            this.fileName = fileName;
            this.read = read;
            this.write = write;
            this.deque = deque;
            this.executionId = executionId;
        }

        @Override
        public void run() {
            long l = System.currentTimeMillis();
            try {

                ExcelWriter excelWriter = EasyExcel.write(fileName, data.getClass()).build();
                WriteSheet writeSheet = EasyExcel.writerSheet("sheet").build();
                int sheetNum = 1;
                int sheetCount = 0;
                while (read.getCount() > 0 || !deque.isEmpty()) {
                    if (!deque.isEmpty()) {
                        List<T> resList = new ArrayList<>();
                        deque.drainTo(resList);
                        if (resList.size() + sheetCount > SpreadsheetVersion.EXCEL2007.getLastRowIndex()) {
                            log.info("当前sheet已经达到上限数量，将切换sheet进行写入");
                            writeSheet = EasyExcel.writerSheet(writeSheet.getSheetName() + "_" + sheetNum++).build();
                            sheetCount = 0;
                        }
                        sheetCount += resList.size();
                        excelWriter.write(resList, writeSheet);
                    }

                }
                excelWriter.finish();
            } catch (Exception e) {
                log.error("出错", e);
                throw e;
            } finally {
                System.out.println(Thread.currentThread().getName() + "——WriteThread耗时" + (System.currentTimeMillis() - l) + "ms");
                write.countDown();
                //这边还可以设置一些回调通知的方法
                System.out.println("任务" + executionId + "结束，执行回调方法...");
            }

        }
    }

    class ExportReadThread implements Runnable {
        private final PageQuery<T> query;
        private final AtomicInteger begin;
        private final AtomicInteger end;
        private final CountDownLatch read;
        private final BlockingQueue<T> deque;

        public ExportReadThread(PageQuery<T> query, AtomicInteger begin, AtomicInteger end, CountDownLatch read, BlockingQueue<T> deque) {
            this.query = query;
            this.begin = begin;
            this.end = end;
            this.read = read;
            this.deque = deque;
        }

        @SneakyThrows
        @Override
        public void run() {
            long l = System.currentTimeMillis();
            int range;
            while (begin.get() < end.get()) {
                int res;
                //设置每个读任务的范围是 队列余量/当前活动的读线程
                range = (CAPACITY - deque.size()) / (int) read.getCount();
                //获取该读任务的读取上限
                res = Math.min(begin.get() + range, end.get());
                int start = begin.get();
                boolean b = begin.compareAndSet(begin.get(), res);
                if (b) {
                    List<T> page = query.page(start, res);
                    for (T temp : page) {
                        deque.put(temp);
                    }
                }
            }
            System.out.println(Thread.currentThread().getName() + "——ReadThread读取耗时" + (System.currentTimeMillis() - l) + "ms");
            read.countDown();

        }
    }

}
