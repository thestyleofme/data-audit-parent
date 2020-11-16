package com.github.thestyleofme.data.comparison.transform.pojo;

import java.util.ArrayList;
import java.util.List;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/10/19 13:52
 * @since 1.0.0
 */
@Data
@Slf4j
public class Bloom {

    private final int bitSize;
    private final int expectedSize;
    private final int numHashFunctions;
    /**
     * 默认的误判率
     */
    private static final double FPP = 0.001;

    public Bloom(int expectedSize) {
        if (expectedSize <= 0) {
            throw new IllegalStateException("Bloom expectedSize must > 0");
        }
        this.expectedSize = expectedSize;
        // 16M = 2^10*2^10*16*8 bit
        this.bitSize = optimalNumOfBits(expectedSize, FPP);
        this.numHashFunctions = optimalNumOfHashFunctions(expectedSize, bitSize);
    }

    public Bloom(int expectedSize, double fpp) {
        if (expectedSize <= 0) {
            throw new IllegalStateException("Bloom expectedSize must > 0");
        }
        this.expectedSize = expectedSize;
        // 16M = 2^10*2^10*16*8 bit
        this.bitSize = optimalNumOfBits(expectedSize, fpp);
        this.numHashFunctions = optimalNumOfHashFunctions(expectedSize, bitSize);
    }

    /**
     * 计算hash方法执行次数
     *
     * @param n expectedSize
     * @param m bitSize
     */
    private int optimalNumOfHashFunctions(long n, long m) {
        return Math.max(1, (int) Math.round((double) m / n * Math.log(2)));
    }

    /**
     * 计算bit位数
     *
     * @param n expectedSize
     * @param p fpp
     */
    private int optimalNumOfBits(long n, double p) {
        if (p == 0) {
            p = Double.MIN_VALUE;
        }
        return (int) (-n * Math.log(p) / (Math.log(2) * Math.log(2)));
    }

    /**
     * 生成hash值集合
     *
     * @param value value
     * @return List
     */
    public List<Integer> doHash(String value, int seed) {
        ArrayList<Integer> list = new ArrayList<>(numHashFunctions);
        for (int i = 0; i < numHashFunctions; i++) {
            seed *= (i + 1);
            int hash = hash(value, seed);
            list.add(hash);
        }
        return list;
    }

    /**
     * hash算法，seed应该是质数 这样分布均匀点
     *
     * @param value value
     * @param seed  seed
     * @return int
     */
    private int hash(String value, int seed) {
        int result = (int) (Math.log(3) * seed) + value.hashCode();
        return Math.abs(result) % bitSize;
    }

}
