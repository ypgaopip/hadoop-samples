package org.example.hbase.basic;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WALEdit;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class RegionObserverDemo implements RegionObserver, RegionCoprocessor {

    private static final Log LOG = LogFactory.getLog(RegionObserverDemo.class);
    private RegionCoprocessorEnvironment env = null;
    // 设定只有info族下的列才能被操作，且name列只写，age列只读
    private static final String FAMILY_NAME = "info";
    private static final String ONLY_PUT_COL = "name";
    private static final String ONLY_READ_COL = "age";

    //2.0加入该方法，否则无法生效
    @Override
    public Optional<RegionObserver> getRegionObserver() {
        // Extremely important to be sure that the coprocessor is invoked as a RegionObserver
        return Optional.of(this);
    }

    // 协处理器是运行于region中的，每一个region都会加载协处理器
    // 这个方法会在regionserver打开region时候执行（还没有真正打开）
    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
        env = (RegionCoprocessorEnvironment) e;
    }

    // 这个方法会在regionserver关闭region时候执行（还没有真正关闭）
    @Override
    public void stop(CoprocessorEnvironment e) throws IOException {
        // nothing to do here
    }

    /**
     * 需求 1.不允许插入age列 2.只能插入name列 3.插入的数据必须为整数 4.插入name列的时候自动插入age列
     */
    @Override
    public void prePut(final ObserverContext<RegionCoprocessorEnvironment> e,
                       final Put put, final WALEdit edit, final Durability durability)
            throws IOException {
        // 首先查看单个put中是否有对只读列有写操作
        List<Cell> cells = put.get(Bytes.toBytes(FAMILY_NAME),
                Bytes.toBytes(ONLY_READ_COL));
        if (cells != null && cells.size() != 0) {
            LOG.warn("User is not allowed to write read_only col.");
            throw new IOException("User is not allowed to write read_only col.");
        }
        // 检查name列
        cells = put.get(Bytes.toBytes(FAMILY_NAME),
                Bytes.toBytes(ONLY_PUT_COL));
        if (cells == null || cells.size() == 0) {
            // 当不存在对于name列的操作的时候则不做任何的处理，直接放行即可
            LOG.info("No Name col operation, just do it.");
            return;
        }
        // 当name列存在的情况下在进行值得检查，查看是否插入了整数
        byte[] aValue = null;
        for (Cell cell : cells) {
            try {
                aValue = CellUtil.cloneValue(cell);
                LOG.warn("aValue = " + Bytes.toString(aValue));
                Integer.valueOf(Bytes.toString(aValue));
            } catch (Exception e1) {
                LOG.warn("Can not put un number value to name col.");
                throw new IOException("Can not put un number value to name col.");
            }
        }
        // 当一切都ok的时候再去构建age列的值，因为按照需求，插入name列的时候需要同时插入age列
        LOG.info("age col also been put value!");
        put.addColumn(Bytes.toBytes(FAMILY_NAME),
                Bytes.toBytes(ONLY_READ_COL), aValue);
    }

    /**
     * 需求 1.不能删除age列 2.只能删除name列 3.删除name列的时候需要一并删除age列
     */
    @Override
    public void preDelete(
            final ObserverContext<RegionCoprocessorEnvironment> e,
            final Delete delete, final WALEdit edit, final Durability durability)
            throws IOException {
        // 首先查看是否对于age列进行了指定删除
        List<Cell> cells = delete.getFamilyCellMap().get(
                Bytes.toBytes(FAMILY_NAME));
        if (cells == null || cells.size() == 0) {
            // 如果客户端没有针对于FAMILY_NAME列族的操作则不用关心，让其继续操作即可。
            LOG.info("NO info family operation ,just do it.");
            return;
        }
        // 开始检查info列族内的操作情况
        byte[] qualifierName = null;
        boolean aDeleteFlg = false;
        for (Cell cell : cells) {
            qualifierName = CellUtil.cloneQualifier(cell);
            // 检查是否对age列进行了删除，这个是不允许的
            if (Arrays.equals(qualifierName, Bytes.toBytes(ONLY_READ_COL))) {
                LOG.info("Can not delete read only age col.");
                throw new IOException("Can not delete read only age col.");
            }
            // 检查是否存在对于name队列的删除
            if (Arrays.equals(qualifierName, Bytes.toBytes(ONLY_PUT_COL))) {
                LOG.info("there is name col in delete operation!");
                aDeleteFlg = true;
            }
        }
        // 如果对于name列有删除，则需要对age列也要删除
        if (aDeleteFlg) {
            LOG.info("age col also been deleted!");
            delete.addColumn(Bytes.toBytes(FAMILY_NAME), Bytes.toBytes(ONLY_READ_COL));
        }
    }


    /**


     result


     */
}
